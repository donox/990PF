import json
from utils.io_utils import read_json, write_json, load_function
from utils.logger import setup_logger
import os

working_dir = '/home/don/Documents/Temp/WW990/structure/'
logs_dir = os.path.join(working_dir, 'logs/')
input_dir = os.path.join(working_dir, 'input/')
intermediates_dir = os.path.join(working_dir, 'intermediates/')

class PipelineManager:
    def __init__(self, config_file):
        self.pipeline_log = os.path.join(logs_dir, 'pipeline.log')
        self.logger = setup_logger("pipeline", self.pipeline_log)
        self.pipeline_config = self._load_config(config_file)
        self.intermediate_folder = "data/intermediate_data/"

    def _load_config(self, config_file):
        """Loads the pipeline configuration from a file."""
        with open(config_file, 'r') as file:
            return json.load(file)["pipeline"]

    def execute_operation(self, step_config, input_data):
        """Executes a single operation as defined in the pipeline configuration."""
        step_name = step_config["step_name"]
        self.logger.info(f"Executing {step_name}.")

        try:
            # Dynamically load the function
            operation = load_function(step_config["module"], step_config["function"])
            output_data = operation(input_data)

            # Write intermediate data if enabled for this step
            if step_config.get("use_intermediate_file", False):
                output_file = f"{self.intermediate_folder}/{step_name}_output.json"
                write_json(output_data, output_file)
                self.logger.info(f"Intermediate data written to {output_file}.")

            return output_data
        except Exception as e:
            self.logger.error(f"Error during {step_name}: {e}")
            raise

    def run_pipeline(self, input_file, output_file):
        """Runs the pipeline as defined in the configuration."""
        try:
            input_data = read_json(input_file)

            for step_config in self.pipeline_config:
                input_data = self.execute_operation(step_config, input_data)

            write_json(input_data, output_file)
            self.logger.info(f"Pipeline execution completed. Final output written to {output_file}.")
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise

if __name__ == "__main__":
    pipeline = PipelineManager(config_file="config/pipeline_config.json")
    infile = os.path.join(input_dir, "input_data.json")
    outfile = os.path.join(intermediates_dir, "intermediate_data.json")
    pipeline.run_pipeline(infile, outfile)
