const path = require('path');

module.exports = {
  mode: 'production',
  entry: './client/index.js',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'client.js',
    library: {
      type: 'commonjs2'
    }
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
  },
  externals: {
    'camunda-modeler-plugin-helpers': 'commonjs2 camunda-modeler-plugin-helpers',
    'camunda-bpmn-moddle': 'commonjs2 camunda-bpmn-moddle'
  }
};
