import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ 0xffffffff) >>> 0;
}

async function files(root, prefix = "") {
  const entries = await readdir(root, { withFileTypes: true });
  const result = [];
  for (const entry of entries) {
    const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
    const path = join(root, entry.name);
    if (entry.isDirectory()) result.push(...await files(path, relative));
    else result.push([relative, await readFile(path)]);
  }
  return result;
}

const sources = await files("dist");
const local = [];
const central = [];
let offset = 0;
for (const [name, data] of sources) {
  const nameBytes = Buffer.from(name, "utf8");
  const header = Buffer.alloc(30 + nameBytes.length);
  header.writeUInt32LE(0x04034b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt32LE(0, 6);
  header.writeUInt16LE(0, 8);
  header.writeUInt32LE(crc32(data), 14);
  header.writeUInt32LE(data.length, 18);
  header.writeUInt32LE(data.length, 22);
  header.writeUInt16LE(nameBytes.length, 26);
  nameBytes.copy(header, 30);
  local.push(header, data);
  const entry = Buffer.alloc(46 + nameBytes.length);
  entry.writeUInt32LE(0x02014b50, 0);
  entry.writeUInt16LE(20, 4);
  entry.writeUInt16LE(20, 6);
  entry.writeUInt32LE(0, 8);
  entry.writeUInt16LE(0, 10);
  entry.writeUInt32LE(crc32(data), 16);
  entry.writeUInt32LE(data.length, 20);
  entry.writeUInt32LE(data.length, 24);
  entry.writeUInt16LE(nameBytes.length, 28);
  entry.writeUInt32LE(offset, 42);
  nameBytes.copy(entry, 46);
  central.push(entry);
  offset += header.length + data.length;
}
const centralBytes = Buffer.concat(central);
const end = Buffer.alloc(22);
end.writeUInt32LE(0x06054b50, 0);
end.writeUInt16LE(sources.length, 8);
end.writeUInt16LE(sources.length, 10);
end.writeUInt32LE(centralBytes.length, 12);
end.writeUInt32LE(offset, 16);
await mkdir("../../artifacts", { recursive: true });
await rm("../../artifacts/fubun-browser-extension.zip", { force: true });
await writeFile("../../artifacts/fubun-browser-extension.zip", Buffer.concat([...local, centralBytes, end]));
