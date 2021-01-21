import React, { ReactElement, useState } from 'react';
import { useLazyQuery, useMutation } from '@apollo/client';
import { InteractionProps, OnSelectProps } from 'react-json-view';
import '@blueprintjs/core/lib/css/blueprint.css';

import { newClient } from './client';

import DataFilter from './components/DataFilter';
import DataView from './components/DataView'; // XXX should rename this
import ApplyResultDialog from './components/ApplyResultDialog';
import { APPLY_JSON_MUTATION, DEFAULT_PATH, DEFAULT_UUID, GET_JSON_QUERY } from './utils/constants';
import { addWildCard, buildNiovaPath, isNumeric } from './utils/misc';

const client = newClient();

function parseData(data: any): any {
    const json = data?.getJson?.json;
    if (!json) {
        return;
    }

    try {
        return JSON.parse(json);
    } catch (e) {
        console.error('Error parsing json', json);
    }

    return;
}

function App(): ReactElement {
    const [uuid, setUuid] = useState<string>(DEFAULT_UUID);
    const [path, setPath] = useState<string>(DEFAULT_PATH);
    const [getJson, { data, error, loading }] = useLazyQuery(GET_JSON_QUERY, {
        fetchPolicy: 'cache-and-network',
        client,
    });

    const [showApplyResults, setShowApplyResults] = useState<boolean>(false);
    const [applyJson, applyJsonResults] = useMutation(APPLY_JSON_MUTATION, { client });

    const doGetJson = ({ path, uuid }: { path: string; uuid: string }) =>
        getJson({ variables: { uuid, path: addWildCard(path) } });

    const onKeySelect = ({ namespace }: OnSelectProps) => {
        console.log('onKeySelect');
        const path = '/' + namespace.filter((o) => !isNumeric(o)).join('/');
        setPath(path);
        doGetJson({ uuid, path });
    };

    const onEdit = ({ existing_src, new_value, namespace, name }: InteractionProps): boolean => {
        console.log('onEdit');
        const path = buildNiovaPath(existing_src, namespace);
        if (path.length) {
            setShowApplyResults(true);
            applyJson({
                variables: { uuid, path: '/' + path.join('/'), value: name + '@' + new_value },
            });
        }

        return false;
    };

    const onApplyResultClose = () => {
        console.log('onApplyResultClose: closing');
        doGetJson({ uuid, path });

        setShowApplyResults(false);
    };

    return (
        <div className="App">
            <DataFilter
                uuid={uuid}
                setUuid={setUuid}
                path={path}
                setPath={setPath}
                onSearch={() => doGetJson({ uuid, path })}
            />
            <DataView
                data={parseData(data)}
                onKeySelect={onKeySelect}
                onEdit={onEdit}
                loading={loading}
                error={error}
            />
            {showApplyResults && (
                <ApplyResultDialog {...applyJsonResults} onClose={onApplyResultClose} />
            )}
        </div>
    );
}

export default App;
