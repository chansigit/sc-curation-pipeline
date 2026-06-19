import dagster as dg

# One dynamic partition per discovered sample folder. Name is fixed and
# referenced by both the sensor (registration) and the asset.
h5ad_partitions = dg.DynamicPartitionsDefinition(name="h5ad_samples")
