# 1.0 DESCRIPTION
This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app). This project provides an interactive and intuitive tool for forecasting energy demand. Detailed steps for re-running the experiment are shown below: 

a. The raw datasets [i.e., energy_dataset.csv, and weather_features.csv] can be downloaded from the following link: https://www.kaggle.com/nicholasjhana/energy-consumption-generation-prices-and-weather
b. Data exploration and union of the two datasets is performed using the ‘Modeling/notebooks/Data_Exploration.scala’ notebook which produces energy_weather_table.csv as the output data file.  
c. Data preparation and modeling is performed using python Jupyter Notebooks for XGBoost, CNN, and LSTM respectively. The notebooks used for modeling are located at ‘Modeling/notebooks/{name}_model.ipynb’. The 'xgboost_model.ipynb' needs to be run first since the output data file ('energy_weather_table_proc.csv') is input for the 'cnn_model.ipynb', 'lstm_model.ipynb' and 'grid_search.ipynb' notebooks.
    - The output from the 'xgboost_model.ipynb' notebook are: 'energy_weather_table_proc.csv', 'energy_weather_table_xgb_preds.csv', 'xgb_wind_feature_importances.json', 
    'xgb_price_feature_importances.json', 'xgb_solar_feature_importances.json', ''xgb_load_feature_importances.json', 'xgb_wind.csv', 'xgb_price.csv', ''xgb_solar.csv' and
    'xgb_load.csv'
    - The output from the 'cnn_model.ipynb' are 'energy_weather_cnn_preds.csv', 'cnn_load.json', 'cnn_price.json', 'cnn_wind.json' and 'cnn_solar.json'. 
    - The output from the 'lstm_model.ipynb' are 'energy_weather_lstm_preds.csv', 'cnn_load.json', 'lstm_price.json', 'lstm_wind.json' and 'lstm_solar.json'
d. Notebook ‘Modeling/notebooks/grid_search.ipynb’ is used to tune the parameters for the XGBoost model. This results in an iterative process for XGBoost modeling. 
e. The ‘Modeling/notebooks/performance_comparison.ipynb’ is used to summarise models performance.

The prediction output from the different models, together with the RMSE output and the feature importance output is processed locally (converted from .json to .csv), column names formatted, rows filtered and columns sorted then these data are passed ('Interactive_dashboard/source/data') to the interactive dashboard for visualization. 
We have included sample data with this package to allow users to run a demonstration of our interactive dashboard using the instructions in 2.0 Installation and 3.0 Execution. 

The interactive dashboard features four (4) main layouts:
1. Interactive Heat Map showing weather data (upper right hand portion of screen): the heat map provides visual representation of various weather parameters which impact electricity demand. Users can hover over highlighted areas to view the value of the selected metric for the selected date. The heat map also has zoom functionality (click to zoom in to selected city, click again to zoom out). 
2. Interactive Bar chart (upper left hand portion of screen) showing top 10 most important features affecting the XGBoost model's performance for total wind generation forecast, total solar generation forecast, total load forecast and price forecast. Feature importance is assessed based on calculated f-score. This bar chart helps users to quickly assess the features which have the highest impact on the forecasted value in question. Where possible, this information can help utility planners to take actions to adjust any of the high impact features which may be within their control. This will be done in order to optimize generation and cost. Users can select different views from a drop down menu and a hover tooltip gives a clear layman's explanation of f-score to aid users in better understanding the dashboard and underlying models. 
3. Interactive Line chart (lower left hand portion of the screen) showing Root Mean Squared Error (RMSE) for the XGBoost, LSTM and CNN models which were used to forecast total load, total solar generation, total wind generation and price. This chart allows users to easily compare model accuracy and thus select the best model and corresponding forecasted value for the value (eg. total load forecast) under consideration. Users can select different views from a drop down menu and a hover tooltip gives a clear layman's explanation of RMSE to aid users in better understanding the dashboard and underlying models. 
4. Interactive Line chart showing day ahead forecasted values for total load, total wind generation, total solar generation, and price. This chart also shows actual total load, actual price and total actual generation for different energy sources. Users can select different time periods (1 week, 1 month, 3 months, 6 months, 1 year) on this chart. Users can mouseover the chart to see values corresponding to the date over which the mouse is located. Users can select different models and view the actual and forecasted values corresponding to that model. This chart allows for easy and intuitive visualization of actual loads against forecasted load and trends in the data. Based on forecasted day ahead loads, forecasted day ahead solar generation and forecasted day ahead wind generation, utility planners can schedule backup load generation as required to meet expected demands.   

# 2.0 INSTALLATION
2.1 Node.js and NPM needs to be installed (free of charge) to run the dashboard. This link https://docs.npmjs.com/downloading-and-installing-node-js-and-npm provides instructions on how to install Node.js and NPM. 

2.2 After Node.js and NPM are installed, navigate to the directory containing the project files. 

2.3 Run 'npm install' in your terminal to install required packages (See package.json for list of required dependencies)

# 3.0 EXECUTION
## Available Scripts

In the project directory, you can run:

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:7700](http://localhost:7700) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `npm test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `npm run build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run eject`

**Note: this is a one-way operation. Once you `eject`, you can’t go back!**

If you aren’t satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you’re on your own.

You don’t have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn’t feel obligated to use this feature. However we understand that this tool wouldn’t be useful if you couldn’t customize it when you are ready for it.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).

### Code Splitting

This section has moved here: [https://facebook.github.io/create-react-app/docs/code-splitting](https://facebook.github.io/create-react-app/docs/code-splitting)

### Analyzing the Bundle Size

This section has moved here: [https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size](https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size)

### Making a Progressive Web App

This section has moved here: [https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app](https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app)

### Advanced Configuration

This section has moved here: [https://facebook.github.io/create-react-app/docs/advanced-configuration](https://facebook.github.io/create-react-app/docs/advanced-configuration)

### Deployment

This section has moved here: [https://facebook.github.io/create-react-app/docs/deployment](https://facebook.github.io/create-react-app/docs/deployment)

### `npm run build` fails to minify

This section has moved here: [https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify](https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify)
