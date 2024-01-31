# HPC-ED Catalog Definition

## Loading HPC-ED metadata using hpc-ed_load_example1.py

The hpc-ed_load_example1.py program can be run by anyone that has authorized credentials
to laod sample data into the an hpc-ed Alpha v1 testing catalog. Use it as an example that
you can modify to load your own training metadata. If your input metadata is in a different
format than the example, the program has a block where you can made the necessary changes.

* My project directory

	$ mkdir myproject
	$ cd myproject

* Get the HPC-ED GitHub repo that has the examples

	$ git clone https://github.com/HPC-ED/HPC-ED_Catalog-Definition.git

* Make PROD symlink in myproject point to that repo

	$ ln -s HPC-ED_Catalog-Definition PROD

* Create a Python3 with the required packages

	$ python3 -m venv mypython
	$ pip install -r PROD/requirements.txt
	$ pip install --upgrade pip
	$ source mypython/bin/activate

* Directories needed by the example

	$ mkdir conf data var

* Copy the config and set the GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET values
* You can modify the PROVIDER_ID to make yourself the publisher/owner of example entries

	$ cp PROD/conf/hpc-ed_load_example1.conf conf/
	$ vi conf/hpc-ed_load_example1.conf

* Run the example

	$ python3 ./PROD/bin/hpc-ed_load_example1.py -c conf/hpc-ed_load_example1.conf -s file:PROD/data/example1_hpc-ed_v1.json -l debug

* Look at the logs

	$ cat var/example1_hpc-ed_v1.log

