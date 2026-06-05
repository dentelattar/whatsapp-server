const fs = require('fs-extra');
const path = require('path');

const CHUNK_SIZE = 512 * 1024; // 512KB per chunk — well under 10MB Firebase limit

class FirebaseStore {
  constructor({ db, userId, dataPath }) {
    this.db = db;
    this.userId = userId;
    this.dataPath = dataPath;
    this.ref = (session) => this.db.ref(`whatsappSessions/${this.userId}/${session}`);
  }

  async sessionExists({ session }) {
    const snap = await this.ref(session).once('value');
    return snap.exists();
  }

  async save({ session }) {
    const zipPath = path.join(this.dataPath, `${session}.zip`);
    const exists = await fs.pathExists(zipPath);
    if (!exists) return;
    const data = await fs.readFile(zipPath);
    const base64 = data.toString('base64');
    const chunks = [];
    for (let i = 0; i < base64.length; i += CHUNK_SIZE) {
      chunks.push(base64.slice(i, i + CHUNK_SIZE));
    }
    const payload = { chunks, count: chunks.length };
    await this.ref(session).set(payload);
  }

  async extract({ session, path: destPath }) {
    const snap = await this.ref(session).once('value');
    if (!snap.exists()) return;
    const val = snap.val();
    let base64;
    if (val.chunks) {
      base64 = val.chunks.join('');
    } else {
      base64 = val;
    }
    const buf = Buffer.from(base64, 'base64');
    await fs.writeFile(destPath, buf);
  }

  async delete({ session }) {
    await this.ref(session).remove();
  }
}

module.exports = FirebaseStore;
