# UVA_NASA_2021

Repository for the UVA Master's in Data Science capstone project in collaboration with NASA. This repository includes scripts and files that do not need to be run on NASA's internal resources to properly compile as well as orienting documents for the project, its course and status. Mostly what is contained here can be designated into four main goals: project orientation, downloading data from iCARE, plotting PARASOL and GRASP data & reformatting PARASOL data into NASA's L1C format.

## Setting up your environment
**Step 1**: Open Anaconda Prompt

**Step 2**: create a new environment - we're using python 3.9.5 specifically
```
conda create -n nasa_capstone_2022 -c anaconda python=3.9.5
```
**Step 3**: activate your new environment
```
conda activate nasa_capstone_2022
```
**Step 4**: install pip (doesn't come preinstalled in your new env)
```
conda install pip
``` 
**Step 5**: navigate to requirements.txt (should be in your base folder)
    
**Step 6**: install requirements.txt
```
pip install -r requirements.txt
```

## Adding new packages to environment
```
conda install package_name
```
OR
```
conda install -c conda-forge package_name
```

Then add it to the requirements.txt
```
conda list -e > requirements.txt
```

## Mac h5py install - if you're encountering errors
```
pip install PEP517
pip install --upgrade wheel
pip install --upgrade setuptools
brew install hdf5
```

## Set up your .env File
* Make a copy of the .env.dist file and name it .env
* Put your specific credentials/paths/whatever in your personal .env file
* The .env file should not be automatically uploaded to github (prevented by .gitignore)
* If you want to add new variables to .env, please add an empty variable in .env.dist

## Adding a kernel to Jupyter Notebook
Make sure that ipykernel is installed
```
conda install ipykernal
```

Then add your environment as a kernel
```
python -m ipykernel install  --name=yourEnvName
```

## Project Orientation
Here we include a folder labeled "Project_Docs" which contains the paper we wrote to complete the capstone project this semester, our final presentation, and a FAQ page with information we wish we knew when we started out. This project took us a lot of time to get up to speed on the background of the problem as well as how figure out how to get started with working with the data and how to use NASA's internal computing resources. Hopefully ,the information in the FAQ gives your group a leg up in this process. 

## Downloading data from iCARE
To do anything meaningful with these files you'll need to first download PARASOL and GRASP data from the iCARE archive (https://www.icare.univ-lille.fr/). The archive has all of the relevant PARASOL and GRASP files formatted as HDF files. It will be good to familiarize yourself with HDF files and how to work with them in python. We have been using the h5py library (link to documentation: https://docs.h5py.org/en/stable/).

To access data from iCARE you will need an iCARE username and password. Apply for one here: https://www.icare.univ-lille.fr/register/

Data acquisition is accomplished via FTP using the icare.py script (written by the NASA team). This script contains code to create an object class "ICARESession" which requires your password and username. The object will then facilitate the acquisition of any files found in iCARE provided you know the internal file path. We have found it helpful to explore the iCARE UI directly on the site to find the locations of files that we want to acquire and then use the ICARESession to easily loop through files we want to download and work with.

For an example of how to use this script see the "Data_Acquisition_Example.ipynb" notebook.
 
## Plotting PARASOL & GRASP data
We have developed a module with all of our plotting functions saved here as "nasa_plotting_scripts.py". Doc strings are included here to peruse. These will allow you to make some quick formatted plots of the PARASOL and GRASP data that you download. See some examples of their usage in the following notebooks:
* GRASP_Exploration.ipynb - Shows how to plot a heatmap of AOD from a GRASP file
*	normalized_rad_retrieval_viz.ipynb - Shows how to plot normalized radiances for a specific file


## Reformatting PARASOL to L1C
One of the next things we expect this project to focus on will be the use of the acquisition algorithm with real world data. To do this we need to process our PARASOL files to a format that NASA's internal computing resources is familiar with. To do this we deconstruct the current PARASOL files, pare them down to only relevant datapoints, transform some fields, reshape the data tensors, and write to a .nc file (another hierarchical data format). You can see the process by which we do this in L1C_Formatting_Center.ipynb. The code in this notebook has also been made into a module labeled "L1C_Centered.py". You can see the example formatted .nc file in "example_centered.nc". 


### Questions
You can forward us any questions you have at the following emails:
* Stephen Whetzel - sjw5ke@virginia.edu (or swhetzel@cubs.com)
* Abhi Dommalapati - ad4bu@virginia.edu 
* Jack Peele - jcp2jf@virginia.edu 
* Sebastian Ranasinghe - sar2jf@virginia.edu  

### Notes
Deprecated_Detritus contains old files I either did not have the time to go through or the heart to delete. Feel free to ignore them. 
