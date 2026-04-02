import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture/overview',
        'architecture/dag-executor',
        'architecture/backend-trait',
      ],
    },
    {
      type: 'category',
      label: 'Backends',
      items: [
        'backends/overview',
        'backends/datafusion',
        'backends/polars',
        'backends/spark-connect',
        'backends/custom-backend',
      ],
    },
    {
      type: 'category',
      label: 'Server',
      items: [
        'server/grpc-api',
        'server/rest-api',
        'server/job-management',
        'server/deployment',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/first-pipeline',
        'guides/dry-run-explain',
      ],
    },
  ],
};

export default sidebars;
