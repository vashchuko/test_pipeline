Test pipeline

In order to start working:
- Create virtual environment
- Activate virtual environment
- Change directory to test-pipeline
- Run `pip install -e ".[dev]"` to install dependencies
- Add your .env file with specified values to the test-pipeline directory 
- Run `dagster dev` to launch UI
- Visit `http://127.0.0.1:3000`, select Overview, select Job for files upload, navigate to Launchpad, Launch job execution
- Check CVAT and add some annotations
- Visit `http://127.0.0.1:3000`, select Overview, select Job for annotations retrieving, navigate to Launchpad, Launch job execution
- Annotations will be saved into folder that is specified in .env file


!NB In this project assumed that docker doesn't have user specified.
