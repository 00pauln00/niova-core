import fs from 'fs';

const CMD_BASE_DIR = '/tmp/.niova/';

type CmdType = 'GET' | 'APPLY';
function getCmd(
    type: CmdType,
    path: string,
    outfile: string,
    where?: string
): string {
    return (
        `${type} ${path}\n` +
        (where ? `WHERE ${where}\n` : '') +
        `OUTFILE /${outfile}\n`
    );
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

function addOutputWatcher(uuid: string, outfile: string): Promise<string> {
    const outputDir = CMD_BASE_DIR + uuid + '/output/';

    return new Promise((resolve) => {
        const watcher = fs.watch(outputDir, (event, filename) => {
            if (filename != outfile) return;

            console.log(outfile, 'event: ' + event, filename);
            watcher.close();

            resolve(fs.readFileSync(outputDir + outfile).toString());
        });
    });
}

export async function cmdJson(
    cmdType: CmdType,
    uuid: string,
    path: string
): Promise<string> {
    const outfile = getOutfile();
    const infile = getInfile(uuid);
    const cmd = getCmd(cmdType, path, outfile);

    const watcher = addOutputWatcher(uuid, outfile);
    console.log('watcher added, writing cmd');
    writeCmd(infile, cmd);

    const resp = await watcher;
    console.log('resp', resp);

    return resp;
}
