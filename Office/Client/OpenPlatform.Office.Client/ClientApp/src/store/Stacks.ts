import { Action, Reducer } from 'redux';
import { AppThunkAction } from './';

// -----------------
// STATE - This defines the type of data maintained in the Redux store.

export interface StacksState {
    isLoading: boolean;
    startDateIndex?: number;
    stacks: Stack[];
}

export interface Stack {
    createdOn: string;
    id: string;
    name: string;
}

// -----------------
// ACTIONS - These are serializable (hence replayable) descriptions of state transitions.
// They do not themselves have any side-effects; they just describe something that is going to happen.

interface RequestStacksAction {
    type: 'REQUEST_STACKS';
    startDateIndex: number;
}

interface ReceiveStacksAction {
    type: 'RECEIVE_STACKS';
    startDateIndex: number;
    stacks: Stack[];
}

// Declare a 'discriminated union' type. This guarantees that all references to 'type' properties contain one of the
// declared type strings (and not any other arbitrary string).
type KnownAction = RequestStacksAction | ReceiveStacksAction;

// ----------------
// ACTION CREATORS - These are functions exposed to UI components that will trigger a state transition.
// They don't directly mutate state, but they can have external side-effects (such as loading data).

export const actionCreators = {
    requestStacks: (startDateIndex: number): AppThunkAction<KnownAction> => (dispatch, getState) => {
        // Only load data if it's something we don't already have (and are not already loading)
        const appState = getState();
        if (appState && appState.stacks && startDateIndex !== appState.stacks.startDateIndex) {
            fetch(`stack`)
                .then(response => response.json() as Promise<Stack[]>)
                .then(data => {
                    dispatch({ type: 'RECEIVE_STACKS', startDateIndex: startDateIndex, stacks: data });
                });

            dispatch({ type: 'REQUEST_STACKS', startDateIndex: startDateIndex });
        }
    }
};

// ----------------
// REDUCER - For a given state and action, returns the new state. To support time travel, this must not mutate the old state.

const unloadedState: StacksState = { stacks: [], isLoading: false };

export const reducer: Reducer<StacksState> = (state: StacksState | undefined, incomingAction: Action): StacksState => {
    if (state === undefined) {
        return unloadedState;
    }

    const action = incomingAction as KnownAction;
    switch (action.type) {
        case 'REQUEST_STACKS':
            return {
                startDateIndex: action.startDateIndex,
                stacks: state.stacks,
                isLoading: true
            };
        case 'RECEIVE_STACKS':
            // Only accept the incoming data if it matches the most recent request. This ensures we correctly handle out-of-order responses.
            if (action.startDateIndex === state.startDateIndex) {
                return {
                    startDateIndex: action.startDateIndex,
                    stacks: action.stacks,
                    isLoading: false
                };
            }
            break;
    }

    return state;
};
