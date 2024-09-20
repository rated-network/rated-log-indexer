# Debug Mode Console Output

This guide explains how to configure the slaOS indexer to output data to the console for debugging purposes.

## Configuration Example

```yaml
output:
  type: console
  console:
    verbose: true
```

## Field Explanations

### Output Config

- `type`: Set to `console` to enable console output.

### Console Output Config

- `verbose`: A boolean flag to enable or disable verbose output. Default is `true`.

## Usage Notes

1. Console output is primarily used for debugging and development purposes.
2. When `verbose` is set to `true`, you'll see more detailed information in the console output.
3. This output type is not recommended for production use, as it doesn't persist data.
4. You can combine console output with other output types for simultaneous logging and data persistence.

## Example Use Case

During development or troubleshooting, you might want to see the processed data directly in your terminal:

```yaml
output:
  type: console
  console:
    verbose: true
```

This configuration will print all processed data to the console with maximum verbosity, allowing you to inspect the data flow in real-time.
