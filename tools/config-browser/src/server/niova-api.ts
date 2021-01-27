import fs from 'fs';

const CMD_BASE_DIR = '/tmp/.niova/';
const DEFAULT_TIMEOUT_MS = 2 * 1000;

type CmdType = 'GET' | 'APPLY';
function buildCmd(type: CmdType, arg: string, outfile: string, where?: string): string {
    return `${type} ${arg}\n` + (where ? `WHERE ${where}\n` : '') + `OUTFILE /${outfile}\n`;
}

function getOutfile(): string {
    return 'outfile' + Math.random();
}

function getInfile(uuid: string): string {
    return CMD_BASE_DIR + uuid + '/input/infile' + Math.random();
}

function writeCmd(path: string, cmd: string): void {
    console.log('writing to', path);
    fs.writeFileSync(path, cmd);
}

function addOutputWatcher(
    uuid: string,
    outfile: string,
    timeout = DEFAULT_TIMEOUT_MS
): Promise<string> {
    const outputDir = CMD_BASE_DIR + uuid + '/output/';
    if (!fs.existsSync(outputDir)) {
        throw new Error('invalid uuid: ' + uuid);
    }

    return new Promise((resolve, reject) => {
        const watcher = fs.watch(outputDir, (event, filename) => {
            if (filename != outfile) {
                return;
            }

            clearTimeout(handle);

            console.log(outfile, 'event: ' + event, filename);
            watcher.close();

            resolve(fs.readFileSync(outputDir + outfile).toString());
        });
        const handle = setTimeout(() => {
            watcher.close();
            reject(new Error('timeout'));
        }, timeout);
    });
}

export async function cmdJson(
    cmdType: CmdType,
    uuid: string,
    arg: string,
    where?: string
): Promise<string> {
    const outfile = getOutfile();
    const infile = getInfile(uuid);
    const cmd = buildCmd(cmdType, arg, outfile, where);

    const watcher = addOutputWatcher(uuid, outfile);
    console.log('watcher added for', outfile, 'writing cmd', cmd);
    writeCmd(infile, cmd);

    const resp = await watcher;

    return resp;
}

interface Service {
    uuid: string;
    uptime: number;
    pid: number;
}

async function getServiceInfo(uuid: string): Promise<Service | false> {
    let json;
    try {
        json = await cmdJson('GET', uuid, '/system_info/.*/.*');
    } catch (e) {
        console.error('Error executing cmd, uuid:', uuid);

        return false;
    }

    let system_info: any;
    try {
        const resp = JSON.parse(json);
        system_info = resp?.system_info;
    } catch (e) {
        console.error('Error parsing json:', json);
    }
    if (!system_info) {
        return false;
    }

    const uptime =
        new Date(system_info.current_time).getTime() - new Date(system_info.start_time).getTime();
    const pid = system_info.pid;

    return {
        uuid,
        uptime,
        pid,
    };
}

export async function getServices(): Promise<Service[]> {
    const dir = fs.opendirSync(CMD_BASE_DIR);
    const servicePromises: Promise<Service | false>[] = [];

    for await (const dirent of dir) {
        if (!dirent.isDirectory()) {
            continue;
        }
        servicePromises.push(getServiceInfo(dirent.name));
    }

    const services = (await Promise.all(servicePromises)).filter(Boolean) as Service[];

    console.log('services', services);

    return services;
}
