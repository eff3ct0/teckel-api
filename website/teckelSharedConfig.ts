import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {themes as prismThemes} from 'prism-react-renderer';

interface TeckelConfigOptions {
  title: string;
  tagline: string;
  baseUrl: string;
  projectName: string;
  githubUrl: string;
  additionalLanguages?: string[];
}

export function createTeckelConfig(options: TeckelConfigOptions): Config {
  const {title, tagline, baseUrl, projectName, githubUrl, additionalLanguages = []} = options;

  return {
    title,
    tagline,
    favicon: 'img/r-favicon.png',
    url: 'https://teckel.rafaelfernandez.dev',
    baseUrl,

    organizationName: 'eff3ct0',
    projectName,

    onBrokenLinks: 'warn',

    future: {
      v4: true,
    },

    markdown: {
      format: 'md',
    },

    i18n: {
      defaultLocale: 'en',
      locales: ['en'],
    },

    headTags: [
      {
        tagName: 'link',
        attributes: {
          rel: 'preconnect',
          href: 'https://fonts.googleapis.com',
        },
      },
      {
        tagName: 'link',
        attributes: {
          rel: 'preconnect',
          href: 'https://fonts.gstatic.com',
          crossorigin: 'anonymous',
        },
      },
      {
        tagName: 'link',
        attributes: {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;450;500;600;700&display=swap',
        },
      },
    ],

    themes: [
      [
        '@easyops-cn/docusaurus-search-local',
        {
          hashed: true,
          indexBlog: false,
        },
      ],
    ],

    presets: [
      [
        'classic',
        {
          docs: {
            routeBasePath: 'docs',
            sidebarPath: './sidebars.ts',
          },
          blog: false,
          theme: {
            customCss: './src/css/custom.css',
          },
        } satisfies Preset.Options,
      ],
    ],

    themeConfig: {
      image: 'img/og-image.png',
      colorMode: {
        defaultMode: 'light',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Teckel',
        logo: {
          alt: 'Teckel Logo',
          src: 'img/r-logo.png',
          srcDark: 'img/r-logo-dark.png',
          href: 'https://teckel.rafaelfernandez.dev',
        },
        items: [
          {
            label: 'Spec',
            href: 'https://teckel.rafaelfernandez.dev/docs/intro',
            position: 'left',
          },
          {
            label: 'Parser',
            href: 'https://teckel.rafaelfernandez.dev/rs/docs/intro',
            position: 'left',
          },
          {
            label: 'Engine',
            href: 'https://teckel.rafaelfernandez.dev/api/docs/intro',
            position: 'left',
          },
          {
            label: 'Editor',
            href: 'https://teckel.rafaelfernandez.dev/ui/docs/intro',
            position: 'left',
          },
          {
            href: githubUrl,
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'Ecosystem',
            items: [
              {
                label: 'Teckel Spec',
                href: 'https://teckel.rafaelfernandez.dev/docs/intro',
              },
              {
                label: 'Teckel Parser',
                href: 'https://teckel.rafaelfernandez.dev/rs/docs/intro',
              },
              {
                label: 'Teckel Engine',
                href: 'https://teckel.rafaelfernandez.dev/api/docs/intro',
              },
              {
                label: 'Teckel Editor',
                href: 'https://teckel.rafaelfernandez.dev/ui/docs/intro',
              },
            ],
          },
          {
            title: 'Resources',
            items: [
              {
                label: 'GitHub Organization',
                href: 'https://github.com/eff3ct0',
              },
              {
                label: 'License',
                href: `${githubUrl}/blob/main/LICENSE`,
              },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} Teckel Contributors.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.vsDark,
        additionalLanguages: ['bash', 'yaml', 'json', ...additionalLanguages],
      },
    } satisfies Preset.ThemeConfig,
  };
}
