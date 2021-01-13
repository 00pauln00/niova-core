import React, { useState } from 'react';
import GetJson, { SearchParams } from './queries/GetJson';
// import './App.css';
import { newClient } from './client';
import { ApolloProvider } from '@apollo/client';
import DataFilter from './components/DataFilter';
import DataView from './components/DataView';

import '@blueprintjs/core/lib/css/blueprint.css';

const DEFAULT_UUID = '';
const DEFAULT_PATH = '/';

const client = newClient();

function App() {
    const [uuid, setUuid] = useState<string>(DEFAULT_UUID);
    const [path, setPath] = useState<string>(DEFAULT_PATH);
    const [searchParams, setSearchParams] = useState<SearchParams>();

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
                        setPath={(path: string) => {
                            setPath(path);
                            setSearchParams({ uuid, path, useCache: true });
                        }}
                        render={DataView}
                    />
                )}
            </div>
        </ApolloProvider>
    );
}

export default App;
