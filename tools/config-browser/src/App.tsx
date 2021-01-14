import React, { ReactElement, useState } from 'react';
import GetJson, { SearchParams } from './queries/GetJson';
// import './App.css';
import { newClient } from './client';
import { ApolloProvider } from '@apollo/client';
import DataFilter from './components/DataFilter';
import getDataView from './components/DataView';
import { OnSelectProps } from 'react-json-view';

import '@blueprintjs/core/lib/css/blueprint.css';

const DEFAULT_UUID = '';
const DEFAULT_PATH = '/';

const client = newClient();

const isNumeric = (num: any): boolean =>
    typeof num == 'number' || (typeof num == 'string' && !isNaN(+num) && !isNaN(parseFloat(num)));

function App(): ReactElement {
    const [uuid, setUuid] = useState<string>(DEFAULT_UUID);
    const [path, setPath] = useState<string>(DEFAULT_PATH);
    const [searchParams, setSearchParams] = useState<SearchParams>();

    const onKeySelect = ({ namespace }: OnSelectProps) => {
        const path = '/' + namespace.filter((o) => !isNumeric(o)).join('/');
        setPath(path);
        setSearchParams({ uuid, path, useCache: true });
    };

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
                {searchParams && (
                    <GetJson
                        {...searchParams}
                        render={({ data }) => getDataView({ data, onKeySelect })}
                    />
                )}
            </div>
        </ApolloProvider>
    );
}

export default App;
