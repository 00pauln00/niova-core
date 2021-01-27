import React, { ReactElement, useState } from 'react';
import { useLazyQuery, useMutation, useQuery } from '@apollo/client';
import { InteractionProps, OnSelectProps } from 'react-json-view';
import moment from 'moment';

import '@blueprintjs/core/lib/css/blueprint.css';

import { newClient } from './client';

import DataFilter from './components/DataFilter';
import DataView from './components/DataView'; // XXX should rename this
import ApplyResultDialog from './components/ApplyResultDialog';
import {
    APPLY_JSON_MUTATION,
    DEFAULT_PATH,
    DEFAULT_UUID,
    GET_JSON_QUERY,
    GET_SERVICES_QUERY,
} from './utils/constants';
import { addWildCard, buildNiovaPath, isNumeric } from './utils/misc';

const client = newClient();

function parseJsonData(data: any): any {
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

function parseServicesData(data: any): any {
    const services = data?.getServices;

    return (
        services &&
        services.reduce(
            (map: any, { uuid, uptime, pid }: any) => ({
                ...map,
                [uuid]: {
                    uuid,
                    uptime: moment.duration(uptime).humanize(),
                    pid,
                },
            }),
            {}
        )
    );
}

function App(): ReactElement {
    const [uuid, setUuid] = useState<string>(DEFAULT_UUID);
    const [path, setPath] = useState<string>(DEFAULT_PATH);
    const [showApplyResults, setShowApplyResults] = useState<boolean>(false);

    const {
        data: services,
        error: servicesError,
        loading: servicesLoading,
        refetch: servicesRefetch,
    } = useQuery(GET_SERVICES_QUERY, {
        fetchPolicy: 'cache-and-network',
        client,
    });
    const [getJson, { data, error, loading }] = useLazyQuery(GET_JSON_QUERY, {
        fetchPolicy: 'cache-and-network',
        client,
    });
    const [applyJson, applyJsonResults] = useMutation(APPLY_JSON_MUTATION, { client });

    const doGetJson = ({ path, uuid }: { path: string; uuid: string }) =>
        getJson({ variables: { uuid, path: addWildCard(path) } });

    const onKeySelect = ({ namespace }: OnSelectProps) => {
        console.log('onKeySelect');
        const path = '/' + namespace.filter((o) => !isNumeric(o)).join('/');
        setPath(path);
        doGetJson({ uuid, path });
    };

    const onServiceSelect = ({ namespace }: OnSelectProps) => {
        if (!namespace?.length) {
            console.error('invalid namespace', namespace);
        }

        const uuid = namespace.slice(-1)[0];
        if (uuid) {
            setUuid(uuid);
            doGetJson({ uuid, path });
        } else {
            console.error('invalid namespace', namespace);
        }
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

    const dataViewOpts = uuid ?
        {
            data: parseJsonData(data),
            onKeySelect,
            onEdit,
            loading,
            error,
        } :
        {
            data: parseServicesData(services),
            onKeySelect: onServiceSelect,
            loading: servicesLoading,
            error: servicesError,
        };

    return (
        <div className="App">
            <DataFilter
                uuid={uuid}
                setUuid={setUuid}
                path={path}
                setPath={setPath}
                onSearch={() => uuid ? doGetJson({ uuid, path }) : servicesRefetch() }
            />
            {<DataView {...dataViewOpts} />}
            {showApplyResults && (
                <ApplyResultDialog {...applyJsonResults} onClose={onApplyResultClose} />
            )}
        </div>
    );
}

export default App;
