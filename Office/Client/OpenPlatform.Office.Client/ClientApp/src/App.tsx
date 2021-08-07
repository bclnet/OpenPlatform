import * as React from 'react';
import { Route } from 'react-router';
import Layout from './components/Layout';
import Home from './components/Home';
import Counter from './components/Counter';
import Realms from './components/Realms';
import Shards from './components/Shards';
import Stacks from './components/Stacks';

import './custom.css'

export default () => (
    <Layout>
        <Route exact path='/' component={Home} />
        <Route path='/counter' component={Counter} />
        <Route path='/realms/:startDateIndex?' component={Realms} />
        <Route path='/shards/:startDateIndex?' component={Shards} />
        <Route path='/stacks/:startDateIndex?' component={Stacks} />
    </Layout>
);
