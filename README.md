# healthcare-etl-v4

## Overview

This is your new Kedro project with Kedro-Viz and PySpark setup, which was generated using `kedro 0.19.6`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Rules and Guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide.
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention).
* Don't commit data to your repository.
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`.

## How to Set Up Your Environment

1. **Install Conda**

   Follow the installation guide for Conda from [Conda Documentation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).

2. **Create a New Environment**

   Open a terminal and run:

   ```bash
   conda create --name <env_name> python=3.8
   conda activate <env_name>









# healthcare-etl-v4

## Overview

This is your new Kedro project with Kedro-Viz and PySpark setup, which was generated using `kedro 0.19.6`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention)
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Declare any dependencies in `requirements.txt` for `pip` installation.

To install them, run:

```
pip install -r requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```


## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. Install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
