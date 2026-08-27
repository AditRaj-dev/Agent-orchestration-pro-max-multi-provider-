// Zero-dependency pure Node.js PNG generator using zlib
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

function crc32(buf) {
  let crc = -1;
  for (let i = 0; i < buf.length; i++) {
    let byte = buf[i];
    for (let j = 0; j < 8; j++) {
      if ((crc ^ byte) & 1) {
        crc = (crc >>> 1) ^ 0xedb88320;
      } else {
        crc = crc >>> 1;
      }
      byte >>>= 1;
    }
  }
  return (crc ^ -1) >>> 0;
}

function makeChunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length, 0);

  const typeBuf = Buffer.from(type, "ascii");
  const crcBuf = Buffer.alloc(4);
  const crc = crc32(Buffer.concat([typeBuf, data]));
  crcBuf.writeUInt32BE(crc, 0);

  return Buffer.concat([len, typeBuf, data, crcBuf]);
}

function createPng(width, height) {
  const header = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);

  // IHDR
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // 8 bit depth
  ihdr[9] = 6; // RGBA
  ihdr[10] = 0; // compression
  ihdr[11] = 0; // filter
  ihdr[12] = 0; // interlace
  const ihdrChunk = makeChunk("IHDR", ihdr);

  // Raw image scanlines
  const rawRows = [];
  for (let y = 0; y < height; y++) {
    const row = Buffer.alloc(1 + width * 4);
    row[0] = 0; // Filter: None
    for (let x = 0; x < width; x++) {
      const idx = 1 + x * 4;
      // Dark slate gradient background #0F1117 with purple/blue accent #7C5CFF
      const isBorder = x < 40 || x > width - 40 || y < 40 || y > height - 40;
      const isInner = x > 200 && x < width - 200 && y > 200 && y < height - 200;

      if (isBorder) {
        row[idx] = 124; // R
        row[idx + 1] = 92; // G
        row[idx + 2] = 255; // B
        row[idx + 3] = 255; // A
      } else if (isInner) {
        row[idx] = 79;
        row[idx + 1] = 140;
        row[idx + 2] = 255;
        row[idx + 3] = 255;
      } else {
        row[idx] = 22;
        row[idx + 1] = 26;
        row[idx + 2] = 35;
        row[idx + 3] = 255;
      }
    }
    rawRows.push(row);
  }

  const rawData = Buffer.concat(rawRows);
  const compressedData = zlib.deflateSync(rawData);
  const idatChunk = makeChunk("IDAT", compressedData);
  const iendChunk = makeChunk("IEND", Buffer.alloc(0));

  return Buffer.concat([header, ihdrChunk, idatChunk, iendChunk]);
}

const iconsDir = path.resolve(process.cwd(), "src-tauri", "icons");
fs.mkdirSync(iconsDir, { recursive: true });

// Create source 1024x1024 icon
const sourcePng = createPng(1024, 1024);
fs.writeFileSync(path.join(iconsDir, "source.png"), sourcePng);

// Create standard sized placeholder PNGs
fs.writeFileSync(path.join(iconsDir, "32x32.png"), createPng(32, 32));
fs.writeFileSync(path.join(iconsDir, "128x128.png"), createPng(128, 128));
fs.writeFileSync(path.join(iconsDir, "128x128@2x.png"), createPng(256, 256));
fs.writeFileSync(path.join(iconsDir, "icon.png"), createPng(512, 512));

console.log("Placeholder icons generated in src-tauri/icons/");
