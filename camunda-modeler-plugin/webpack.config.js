const path = require('path');

// The plugin's client bundle is loaded into the Camunda Modeler renderer, which does
// NOT provide a Node-style `require`. `camunda-modeler-plugin-helpers` is pure browser
// code (it only touches `window.plugins` / `window.vendor`), so everything is bundled
// here rather than left as a `commonjs`/`commonjs2` external — otherwise the bundle would
// emit a runtime `require("camunda-modeler-plugin-helpers")` and fail with
// "require is not defined". The bundle self-registers via side effects (registerPlatform...),
// so no library export is needed.
module.exports = {
  mode: 'production',
  entry: './client/index.js',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'client.js'
  },
  module: {
    rules: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['@babel/preset-env']
          }
        }
      }
    ]
  },
  target: 'web',
  resolve: {
    extensions: ['.js']
  }
};
