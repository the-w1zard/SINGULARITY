import { type Configuration } from '@rspack/core';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const r = (p: string) => path.resolve(__dirname, p);

const config: Configuration = {
  entry: './src/index.ts',
  target: 'node',    
  externalsPresets: { node: true }, 
  resolve: {
    extensions: ['.ts', '.tsx', '.js', '.jsx'],
    extensionAlias: { '.js': ['.ts', '.js'] },
    alias: {
      '~': r('src'),
      '#STR': '@the-w1zard/app_store',
      '#HPR': '@the-w1zard/prim_helpers',
      '#SS': '@the-w1zard/solutions',
      '#PRIM': '@the-w1zard/prims',
      '#sauron': r('src/index.ts'),
      '#the-cloud': r('src/the-cloud.ts'),
      '#tc': r('src/the-cloud.ts'),
    },
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'builtin:swc-loader',
        exclude: /node_modules/,
      },
      {
        test: /\.(js|jsx|ts|tsx)$/,
        exclude: /node_modules\/(?!@the-w1zard)/,
        use: [
          {
            loader: 'builtin:swc-loader',
            options: {
              jsc: {
                parser: {
                  syntax: 'typescript',
                  tsx: true,
                },
              },
            },
          },
          { loader: path.resolve(__dirname, './sauron-rspack.ts') },
        ],
      }
    ],
  },
  output: {
    path: r('dist'),
    filename: 'index.js',
    library: { type: 'modern-module' },
  },
  optimization: {
    avoidEntryIife: true,
    minimize: false,
  },
};

export default config;
