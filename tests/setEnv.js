const yaml = require('js-yaml');
const path = require('path');
const fs = require('fs');

const filePath = path.join(__dirname, '..', 'configs/providers/aws/local/env.yml')

try {
    const doc = yaml.load(fs.readFileSync(filePath, 'utf8'));
    for (let key of Object.keys(doc)) {
        process.env[key] = doc[key];
    }
} catch (e) {
    console.debug(e);
}
