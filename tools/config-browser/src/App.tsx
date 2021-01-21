import React, { ReactElement, useState } from 'react';
import GetJson, { SearchParams } from './queries/GetJson';
// import './App.css';
import { newClient } from './client';
import { ApolloProvider } from '@apollo/client';
import DataFilter from './components/DataFilter';
import renderDataView from './components/DataView';
import { InteractionProps, OnSelectProps } from 'react-json-view';

import '@blueprintjs/core/lib/css/blueprint.css';
import ApplyJson, { ApplyParams } from './queries/ApplyJson';
import renderApplyResultDialog from './components/ApplyResultDialog';
import { DataLoadingError } from './types';

const DEFAULT_UUID = '';
const DEFAULT_PATH = '/';

const client = newClient();

const isNumeric = (num: any): boolean =>
    typeof num == 'number' || (typeof num == 'string' && !isNaN(+num) && !isNaN(parseFloat(num)));

const niovaEscape = (str: string) => str.replace(/([() ])/, '\\$1');

const NIOVA_ID_KEYS = ['name'];

function buildNiovaPath(item: any, namespace: (string | null)[]): string[] {
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

function App(): ReactElement {
    const [uuid, setUuid] = useState<string>(DEFAULT_UUID);
    const [path, setPath] = useState<string>(DEFAULT_PATH);
    const [searchParams, setSearchParams] = useState<SearchParams>();
    const [applyParams, setApplyParams] = useState<ApplyParams>();

    const onKeySelect = ({ namespace }: OnSelectProps) => {
        console.log('onKeySelect');
        const path = '/' + namespace.filter((o) => !isNumeric(o)).join('/');
        setPath(path);
        setSearchParams({ uuid, path, useCache: true });
    };

    const onEdit = ({ existing_src, new_value, namespace, name }: InteractionProps): boolean => {
        console.log('onEdit');
        const path = buildNiovaPath(existing_src, namespace);
        if (path.length) {
            setApplyParams({ uuid, path: '/' + path.join('/'), value: name + '@' + new_value });
        }

        return false;
    };

    const onApplyResultClose = () => {
        console.log('onApplyResultClose: closing');
        setApplyParams(undefined);
        setSearchParams({ uuid, path, useCache: false });
    };

    console.log('App');

    return (
        <ApolloProvider client={client}>
            <div className="App">
                <DataFilter
                    uuid={uuid}
                    setUuid={setUuid}
                    path={path}
                    setPath={setPath}
                    onSearch={() => setSearchParams({ uuid, path, useCache: false })}
                />
                {searchParams && !applyParams && (
                    <GetJson
                        {...searchParams}
                        render={({ data }) => renderDataView({ data, onKeySelect, onEdit })}
                    />
                )}
                {applyParams && (
                    <ApplyJson
                        {...applyParams}
                        render={({ data, loading, error }: DataLoadingError) =>
                            renderApplyResultDialog({
                                data,
                                loading,
                                error,
                                onClose: onApplyResultClose,
                            })
                        }
                    />
                )}
            </div>
        </ApolloProvider>
    );
}

export default App;
