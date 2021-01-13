import { cmdJson } from './niova-api';

const CMD = process.env['NIOVA_CMD'] || 'GET';
const UUID = process.env['NIOVA_UUID'];
const PATH = process.env['NIOVA_PATH'] || '/.*/.*';

async function main() {
    if (CMD != 'GET' && CMD != 'APPLY') {
        console.error('invalid CMD env param');
        return;
    }
    if (!UUID) {
        console.error('missing UUID env param');
        return;
    }

    await cmdJson(CMD, UUID, PATH);
}
main();
