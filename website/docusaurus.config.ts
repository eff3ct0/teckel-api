import {createTeckelConfig} from './teckelSharedConfig';

export default createTeckelConfig({
  title: 'Teckel Engine',
  tagline: 'Pluggable execution engine for Teckel pipelines',
  baseUrl: '/api/',
  projectName: 'teckel-api',
  githubUrl: 'https://github.com/eff3ct0/teckel-api',
  additionalLanguages: ['rust', 'toml', 'protobuf'],
});
