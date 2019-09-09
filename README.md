# legacy-processor-module

This module is used as a dependency in [legacy-sub-processor](https://github.com/topcoder-platform/legacy-sub-processor) and [legacy-mm-processor](https://github.com/topcoder-platform/legacy-mm-processor)

`legacy-processor-module` depends on its parent project for the following,

1. Configuration options - It is obtained from the `config` folder of the parent project

2. Module dependencies - The parent project should contain all the dependencies necessary for `legacy-processor-module` to operate.

## Application performance management

`legacy-processor-module` also supports application performance management if the tracers in `common/tracers.js` are initialized by the parent project.

The module supports `Datadog`, `LightStep` and `SignalFx` APM tracing.

The configuration options and the required modules (`dd-trace`, `lightstep-tracer` and `signalfx-tracing`) must be present in the parent project for it to work.

## Linting

In order to run the linter, you need to,

1. Include `legacy-processor-module` as a dependency in `legacy-sub-processor` or `legacy-mm-processor`

2. Add `standard` module to the parent project if not already present

3. Run the linter from the parent project, 
    
    `cd node_modules/legacy-processor-module`

    `npm run lint` - To find lint errors

    `npm run lint:fix` - To find and fix lint errors

## Testing

You can test `legacy-processor-module` by running the tests of the parent project to which it belongs. 

The parent uses services of `legacy-processor-module` and should validate the results of using those services in its tests.