#!/usr/bin/env python3
"""
Demo script để chạy từng stage riêng biệt của pipeline
Giúp debug dễ hơn khi gặp lỗi
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta


def run_command(cmd, description):
    """Run command and show result"""
    print(f"\n{'='*60}")
    print(f"🔧 {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__)
        )

        if result.stdout:
            print("✅ Output:")
            print(result.stdout)

        if result.stderr:
            print("❌ Errors:")
            print(result.stderr)

        if result.returncode == 0:
            print(f"✅ {description} completed successfully!")
        else:
            print(f"❌ {description} failed with return code {result.returncode}")

        return result.returncode == 0

    except Exception as e:
        print(f"💥 Exception running {description}: {e}")
        return False


def main():
    """Demo các cách chạy pipeline với stage control"""

    # Date for testing (yesterday)
    test_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    property_type = "house"

    print(
        f"""
🚀 PIPELINE STAGE DEMO
=====================
Testing date: {test_date}
Property type: {property_type}

Available stage control options:
1. --extract-only      : Chỉ chạy extraction (Raw → Bronze)
2. --transform-only    : Chỉ chạy transformation (Bronze → Silver)
3. --unify-only        : Chỉ chạy unification (Silver → Gold)
4. --skip-extraction   : Bỏ qua extraction stage
5. --skip-transformation : Bỏ qua transformation stage
6. --skip-unification  : Bỏ qua unification stage

Examples of usage:
"""
    )

    # Example commands
    examples = [
        {
            "desc": "Run only extraction stage",
            "cmd": [
                "python",
                "daily_processing.py",
                "--date",
                test_date,
                "--property-types",
                property_type,
                "--extract-only",
            ],
        },
        {
            "desc": "Run only transformation stage (requires bronze data)",
            "cmd": [
                "python",
                "daily_processing.py",
                "--date",
                test_date,
                "--property-types",
                property_type,
                "--transform-only",
            ],
        },
        {
            "desc": "Run only unification stage (requires silver data)",
            "cmd": [
                "python",
                "daily_processing.py",
                "--date",
                test_date,
                "--property-types",
                property_type,
                "--unify-only",
            ],
        },
        {
            "desc": "Run full pipeline excluding extraction",
            "cmd": [
                "python",
                "daily_processing.py",
                "--date",
                test_date,
                "--property-types",
                property_type,
                "--skip-extraction",
            ],
        },
        {
            "desc": "Run only extraction + transformation (skip unification)",
            "cmd": [
                "python",
                "daily_processing.py",
                "--date",
                test_date,
                "--property-types",
                property_type,
                "--skip-unification",
            ],
        },
    ]

    # Show examples
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example['desc']}:")
        print(f"   {' '.join(example['cmd'])}")
        print()

    # Interactive demo
    print("Choose a demo to run (1-5) or 'q' to quit:")

    while True:
        choice = input("\nEnter choice (1-5, 'q' to quit): ").strip().lower()

        if choice == "q":
            print("👋 Bye!")
            break

        try:
            choice_num = int(choice)
            if 1 <= choice_num <= len(examples):
                example = examples[choice_num - 1]
                success = run_command(example["cmd"], example["desc"])

                if success:
                    print(f"\n🎉 Demo {choice_num} completed successfully!")
                else:
                    print(f"\n💥 Demo {choice_num} failed!")

            else:
                print("❌ Invalid choice. Please enter 1-5 or 'q'")

        except ValueError:
            print("❌ Invalid input. Please enter a number 1-5 or 'q'")


if __name__ == "__main__":
    main()
