const fs = require('fs-extra');
const path = require('path');

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
    await this.ref(session).set(data.toString('base64'));
  }

  async extract({ session, path: destPath }) {
    const snap = await this.ref(session).once('value');
    if (!snap.exists()) return;
    const buf = Buffer.from(snap.val(), 'base64');
    await fs.writeFile(destPath, buf);
  }

  async delete({ session }) {
    await this.ref(session).remove();
  }
}

module.exports = FirebaseStore;
