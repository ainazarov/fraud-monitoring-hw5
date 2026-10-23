"""Create an initial model through the same checked pipeline used by Airflow."""
from pipeline.registry import ModelRegistry
from pipeline.training import prepare_window, train_candidate, evaluate_candidate, publish_candidate


if __name__ == "__main__":
    if not ModelRegistry().current():
        result = publish_candidate(evaluate_candidate(train_candidate(prepare_window("bootstrap"))))
        print(result)
    else:
        print("Existing active model preserved")
