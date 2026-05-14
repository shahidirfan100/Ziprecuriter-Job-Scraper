import prettier from 'eslint-config-prettier';

import apify from '@apify/eslint-config/js.js';

/* eslint-disable import-x/no-default-export */
export default [{ ignores: ['**/dist'] }, ...apify, prettier];
