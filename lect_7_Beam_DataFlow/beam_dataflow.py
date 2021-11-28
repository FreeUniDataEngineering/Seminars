"""'

Quickstart: https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

pip install wheel
pip install 'apache-beam[gcp]'

# Runs locally
python -m apache_beam.examples.wordcount --output outputs

# Submit a Beam job to DataFlow
python -m apache_beam.examples.wordcount \
    --region europe-west6 \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://yet_another_bucket/results/king_lear \
    --runner DataflowRunner \
    --project complete-verve-325920 \
    --temp_location gs://yet_another_bucket/tmp/

python -m apache_beam.examples.wordcount --region europe-west6 --input gs://dataflow-samples/shakespeare/kinglear.txt --output gs://yet_another_bucket/results/outputs --runner DataflowRunner --project complete-verve-325920 --temp_location gs://yet_another_bucket/tmp/


"""

