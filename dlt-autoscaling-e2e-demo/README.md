# DLT Auto-Scaling End-to-End Demo

This demo showcases how Delta Live Tables (DLT) and Lakeflow declarative pipelines automatically scale when input data volume changes.

## Demo Architecture

```
Data Generators → DLT Pipeline → Monitoring Dashboard
     ↓               ↓              ↓
  Variable        Auto-Scale    Performance
  Volume          Resources      Metrics
```

## Folder Structure

- **`pipelines/`** - DLT pipeline definitions and configurations
- **`data-generators/`** - Scripts to generate variable volume data streams
- **`monitoring/`** - Real-time monitoring and metrics collection
- **`docs/`** - Documentation and setup guides
- **`configs/`** - Configuration files and parameter settings

## Demo Scenarios

1. **Low Volume** - 1K events/second → Pipeline scales down
2. **Medium Volume** - 10K events/second → Pipeline maintains resources
3. **High Volume** - 100K events/second → Pipeline scales up automatically
4. **Burst Traffic** - Sudden spikes → Pipeline handles elastically

## Key Features Demonstrated

- ✅ **Automatic Scaling** - DLT detects volume changes and adjusts resources
- ✅ **Cost Optimization** - Resources scale down during low traffic
- ✅ **Performance Monitoring** - Real-time metrics and alerting
- ✅ **Declarative Pipelines** - Simple SQL/Python pipeline definitions
- ✅ **Zero Manual Intervention** - Fully automated scaling decisions

## Getting Started

See `docs/setup-guide.md` for complete setup instructions.