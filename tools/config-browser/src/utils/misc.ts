export const isNumeric = (num: any): boolean =>
    typeof num == 'number' || (typeof num == 'string' && !isNaN(+num) && !isNaN(parseFloat(num)));

export const niovaEscape = (str: string) => str.replace(/([() ])/, '\\$1');

const NIOVA_ID_KEYS = ['name'];
export function buildNiovaPath(item: any, namespace: (string | null)[]): string[] {
    const path: string[] = [];

    for (let i = 0; i < namespace.length; i++) {
        const key = namespace[i];
        if (key == null) {
            console.error('null found in namespace', namespace);

            return [];
        }

        item = item[key];
        // NIOVA uses field filters instead of array indexes
        if (isNumeric(key)) {
            let newKey: string | false = false;
            for (let j = 0; j < NIOVA_ID_KEYS.length; j++) {
                const id = NIOVA_ID_KEYS[j];
                if (item[id]) {
                    newKey = `${id}@${niovaEscape(item[id])}`;
                    break;
                }
            }
            if (!newKey) {
                console.error('array element found with no name-type field', namespace, item);

                return [];
            } else {
                path.push(newKey);
            }
        } else {
            path.push(key);
        }
    }

    return path;
}

export function addWildCard(path: string) {
    if (path.includes('.*')) {
        return path;
    }

    let new_path = path;
    if (path[path.length - 1] != '/') {
        new_path += '/';
    }

    return new_path + '/.*/.*';
}
