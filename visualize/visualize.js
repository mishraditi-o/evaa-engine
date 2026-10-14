import {
  asyncBufferFromFile,
  parquetMetadataAsync,
  parquetReadObjects
} from 'hyparquet';

const CHUNK_ROWS = 500000;

const DEFAULTS = {
  timeBins: 200,   // points in the event-rate chart
  gridSize: 128,   // longest side of the heatmap / frame grids
  frameCount: 20   // number of frames in the player
};

const COLUMNS = ['t', 'x', 'y', 'p'];

function positiveInt(value, fallback, name, max) {
  if (value === undefined) return fallback;

  if (!Number.isInteger(value) || value < 1 || value > max) {
    throw new Error(`${name} must be an integer between 1 and ${max}`);
  }

  return value;
}

// Reads the Parquet file in chunks so memory stays small for big files.
async function forEachChunk(file, totalRows, callback) {
  for (let start = 0; start < totalRows; start += CHUNK_ROWS) {
    const rows = await parquetReadObjects({
      file,
      columns: COLUMNS,
      rowStart: start,
      rowEnd: Math.min(start + CHUNK_ROWS, totalRows)
    });

    callback(rows);
  }
}

/*
 * visualize(inputPath, options)
 *
 * inputPath: the data.parquet file made by ingest().
 * options:   { timeBins, gridSize, frameCount, width, height }
 *
 * Returns binned data only (never raw events), so the result stays
 * small however large the dataset is. Errors are thrown.
 */
export async function visualize(inputPath, options = {}) {
  if (!inputPath) {
    throw new Error('inputPath is required');
  }

  if (!inputPath.toLowerCase().endsWith('.parquet')) {
    throw new Error('visualize() needs the data.parquet file made by ingest()');
  }

  const timeBins = positiveInt(options.timeBins, DEFAULTS.timeBins, 'timeBins', 2000);
  const gridSize = positiveInt(options.gridSize, DEFAULTS.gridSize, 'gridSize', 512);
  const frameCount = positiveInt(options.frameCount, DEFAULTS.frameCount, 'frameCount', 200);

  const file = await asyncBufferFromFile(inputPath);
  const parquet = await parquetMetadataAsync(file);
  const totalRows = Number(parquet.num_rows);

  if (totalRows === 0) {
    throw new Error('The dataset has no events');
  }

  // Pass 1: time range and sensor size
  let minT = Infinity;
  let maxT = -Infinity;
  let maxX = -1;
  let maxY = -1;

  await forEachChunk(file, totalRows, (rows) => {
    for (const r of rows) {
      if (r.t < minT) minT = r.t;
      if (r.t > maxT) maxT = r.t;
      if (r.x > maxX) maxX = r.x;
      if (r.y > maxY) maxY = r.y;
    }
  });

  const width = options.width ?? maxX + 1;
  const height = options.height ?? maxY + 1;
  const durationUs = maxT - minT;
  const span = durationUs > 0 ? durationUs : 1;

  // Grid keeps the sensor aspect ratio; the longest side is gridSize.
  const scale = Math.min(1, gridSize / Math.max(width, height));
  const gridW = Math.max(1, Math.ceil(width * scale));
  const gridH = Math.max(1, Math.ceil(height * scale));
  const cells = gridW * gridH;

  const rateOn = new Uint32Array(timeBins);
  const rateOff = new Uint32Array(timeBins);
  const heatmap = new Uint32Array(cells);
  const frames = Array.from({ length: frameCount }, () => new Int32Array(cells));

  let on = 0;
  let off = 0;

  // Pass 2: fill every chart in one go
  await forEachChunk(file, totalRows, (rows) => {
    for (const r of rows) {
      const rel = (r.t - minT) / span;
      const bin = Math.min(timeBins - 1, Math.floor(rel * timeBins));
      const frame = Math.min(frameCount - 1, Math.floor(rel * frameCount));

      const gx = Math.min(gridW - 1, Math.floor(r.x * scale));
      const gy = Math.min(gridH - 1, Math.floor(r.y * scale));
      const cell = gy * gridW + gx;

      const isOn = r.p > 0;

      if (isOn) {
        on++;
        rateOn[bin]++;
        frames[frame][cell]++;
      } else {
        off++;
        rateOff[bin]++;
        frames[frame][cell]--;
      }

      heatmap[cell]++;
    }
  });

  const binUs = span / timeBins;
  const frameUs = span / frameCount;

  return {
    success: true,

    metadata: {
      width,
      height,
      eventCount: totalRows,
      durationUs
    },

    // Events per time bin (ON and OFF kept apart for stacked charts)
    eventRate: {
      binUs,
      on: Array.from(rateOn),
      off: Array.from(rateOff)
    },

    polarity: { on, off },

    // Event count per grid cell, row by row (index = y * gridW + x)
    heatmap: {
      gridW,
      gridH,
      max: heatmap.reduce((a, b) => (b > a ? b : a), 0),
      values: Array.from(heatmap)
    },

    // ON minus OFF per cell for each time window
    frames: frames.map((values, i) => ({
      tStartUs: Math.round(i * frameUs),
      tEndUs: Math.round((i + 1) * frameUs),
      gridW,
      gridH,
      values: Array.from(values)
    })),

    files: []
  };
}
