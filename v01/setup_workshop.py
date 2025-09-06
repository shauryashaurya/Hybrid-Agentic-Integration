# setup_workshop.py
# Initial setup script for the Hybrid Agentic Integration Architectures workshop
# Run this first to ensure environment is ready

import sys
import subprocess
import os


def check_python_version():
    # Ensure Python 3.8 or higher
    if sys.version_info < (3, 8):
        print("Error: Python 3.8 or higher is required")
        sys.exit(1)
    print(f"Python version: {sys.version}")


def install_requirements():
    # Install required packages
    print("\nInstalling required packages...")
    subprocess.check_call([sys.executable, "-m", "pip",
                          "install", "--upgrade", "--no-cache-dir", "-r", "requirements.txt"])
    print("All packages installed successfully")


def setup_environment():
    # Create necessary directories
    os.makedirs('workshop_data', exist_ok=True)
    os.makedirs('agents', exist_ok=True)
    os.makedirs('schemas', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    print("\nCreated workshop directories")

    # Create .env file template if it doesn't exist
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write('# OpenAI API Configuration\n')
            f.write('OPENAI_API_KEY=your_api_key_here\n')
            f.write('# Use gpt-3.5-turbo for cost efficiency in workshop\n')
            f.write('OPENAI_MODEL=gpt-3.5-turbo\n')
        print("Created .env template - please add your OpenAI API key")


def generate_sample_data():
    # Import and run the data generator
    print("\nGenerating sample data...")
    from fake_data_generator import generate_all_data
    generate_all_data()


def verify_setup():
    # Verify all components are ready
    print("\n" + "="*50)
    print("SETUP VERIFICATION")
    print("="*50)

    checks = []

    # Check Python version
    checks.append(("Python 3.8+", sys.version_info >= (3, 8)))

    # Check key packages
    try:
        import pandas
        checks.append(("Pandas", True))
    except ImportError:
        checks.append(("Pandas", False))

    try:
        import openai
        checks.append(("OpenAI", True))
    except ImportError:
        checks.append(("OpenAI", False))

    try:
        import faker
        checks.append(("Faker", True))
    except ImportError:
        checks.append(("Faker", False))

    # Check directories
    checks.append(("Workshop directories", os.path.exists('workshop_data')))

    # Check sample data
    checks.append(("Sample data files", os.path.exists(
        'workshop_data/customers_v1.csv')))

    # Check .env file
    checks.append((".env file", os.path.exists('.env')))

    # Display results
    all_good = True
    for check_name, status in checks:
        status_str = "OK" if status else "FAILED"
        print(f"{check_name:<25} {status_str}")
        if not status:
            all_good = False

    print("="*50)

    if all_good:
        print("\n Setup complete! You're ready to start the workshop.")
        print("\nREMINDER: Add your OpenAI API key to the .env file")
    else:
        print("\nSome checks failed. Please review and fix the issues above.")

    return all_good


def main():
    print("HYBRID AGENTIC INTEGRATION ARCHITECTURES")
    print("Workshop Setup")

    # Run setup steps
    check_python_version()
    install_requirements()
    setup_environment()
    generate_sample_data()

    # Verify everything is ready
    verify_setup()

    print("\nNext steps:")
    print("1. Add your OpenAI API key to .env file")
    print("2. Run 'python module_1_foundations.py' to begin the workshop")


if __name__ == "__main__":
    main()
