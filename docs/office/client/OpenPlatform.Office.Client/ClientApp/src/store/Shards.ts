import { Action, Reducer } from 'redux';
import { AppThunkAction } from './';

// -----------------
// STATE - This defines the type of data maintained in the Redux store.

export interface ShardsState {
    isLoading: boolean;
    startDateIndex?: number;
    shards: Shard[];
}

export interface Shard {
    createdOn: string;
    id: string;
    name: string;
}

// -----------------
// ACTIONS - These are serializable (hence replayable) descriptions of state transitions.
// They do not themselves have any side-effects; they just describe something that is going to happen.

interface RequestShardsAction {
    type: 'REQUEST_SHARDS';
    startDateIndex: number;
}

interface ReceiveShardsAction {
    type: 'RECEIVE_SHARDS';
    startDateIndex: number;
    shards: Shard[];
}

// Declare a 'discriminated union' type. This guarantees that all references to 'type' properties contain one of the
// declared type strings (and not any other arbitrary string).
type KnownAction = RequestShardsAction | ReceiveShardsAction;

// ----------------
// ACTION CREATORS - These are functions exposed to UI components that will trigger a state transition.
// They don't directly mutate state, but they can have external side-effects (such as loading data).

export const actionCreators = {
    requestShards: (startDateIndex: number): AppThunkAction<KnownAction> => (dispatch, getState) => {
        // Only load data if it's something we don't already have (and are not already loading)
        const appState = getState();
        if (appState && appState.shards && startDateIndex !== appState.shards.startDateIndex) {
            fetch(`shard`)
                .then(response => response.json() as Promise<Shard[]>)
                .then(data => {
                    dispatch({ type: 'RECEIVE_SHARDS', startDateIndex: startDateIndex, shards: data });
                });

            dispatch({ type: 'REQUEST_SHARDS', startDateIndex: startDateIndex });
        }
    }
};

// ----------------
// REDUCER - For a given state and action, returns the new state. To support time travel, this must not mutate the old state.

const unloadedState: ShardsState = { shards: [], isLoading: false };

export const reducer: Reducer<ShardsState> = (state: ShardsState | undefined, incomingAction: Action): ShardsState => {
    if (state === undefined) {
        return unloadedState;
    }

    const action = incomingAction as KnownAction;
    switch (action.type) {
        case 'REQUEST_SHARDS':
            return {
                startDateIndex: action.startDateIndex,
                shards: state.shards,
                isLoading: true
            };
        case 'RECEIVE_SHARDS':
            // Only accept the incoming data if it matches the most recent request. This ensures we correctly handle out-of-order responses.
            if (action.startDateIndex === state.startDateIndex) {
                return {
                    startDateIndex: action.startDateIndex,
                    shards: action.shards,
                    isLoading: false
                };
            }
            break;
    }

    return state;
};
