import { Action, Reducer } from 'redux';
import { AppThunkAction } from './';

// -----------------
// STATE - This defines the type of data maintained in the Redux store.

export interface RealmsState {
    isLoading: boolean;
    startDateIndex?: number;
    realms: Realm[];
}

export interface Realm {
    createdOn: string;
    id: string;
    name: string;
}

// -----------------
// ACTIONS - These are serializable (hence replayable) descriptions of state transitions.
// They do not themselves have any side-effects; they just describe something that is going to happen.

interface RequestRealmsAction {
    type: 'REQUEST_REALMS';
    startDateIndex: number;
}

interface ReceiveRealmsAction {
    type: 'RECEIVE_REALMS';
    startDateIndex: number;
    realms: Realm[];
}

// Declare a 'discriminated union' type. This guarantees that all references to 'type' properties contain one of the
// declared type strings (and not any other arbitrary string).
type KnownAction = RequestRealmsAction | ReceiveRealmsAction;

// ----------------
// ACTION CREATORS - These are functions exposed to UI components that will trigger a state transition.
// They don't directly mutate state, but they can have external side-effects (such as loading data).

export const actionCreators = {
    requestRealms: (startDateIndex: number): AppThunkAction<KnownAction> => (dispatch, getState) => {
        // Only load data if it's something we don't already have (and are not already loading)
        const appState = getState();
        if (appState && appState.realms && startDateIndex !== appState.realms.startDateIndex) {
            fetch(`realm`)
                .then(response => response.json() as Promise<Realm[]>)
                .then(data => {
                    dispatch({ type: 'RECEIVE_REALMS', startDateIndex: startDateIndex, realms: data });
                });

            dispatch({ type: 'REQUEST_REALMS', startDateIndex: startDateIndex });
        }
    }
};

// ----------------
// REDUCER - For a given state and action, returns the new state. To support time travel, this must not mutate the old state.

const unloadedState: RealmsState = { realms: [], isLoading: false };

export const reducer: Reducer<RealmsState> = (state: RealmsState | undefined, incomingAction: Action): RealmsState => {
    if (state === undefined) {
        return unloadedState;
    }

    const action = incomingAction as KnownAction;
    switch (action.type) {
        case 'REQUEST_REALMS':
            return {
                startDateIndex: action.startDateIndex,
                realms: state.realms,
                isLoading: true
            };
        case 'RECEIVE_REALMS':
            // Only accept the incoming data if it matches the most recent request. This ensures we correctly handle out-of-order responses.
            if (action.startDateIndex === state.startDateIndex) {
                return {
                    startDateIndex: action.startDateIndex,
                    realms: action.realms,
                    isLoading: false
                };
            }
            break;
    }

    return state;
};
