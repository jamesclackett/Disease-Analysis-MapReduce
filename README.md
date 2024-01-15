# MapReduce Disease Analysis

---
A parallelised system that runs Map Reduce jobs on a disease dataset (https://www.kaggle.com/datasets/niyarrbarman/symptom2disease). 

The disease dataset has been copied and hosted privately on an Amazon S3 Bucket, along with copies of the Java MapReduce code.
The dataset contains 24 different diseases, and for each there are 50 textual (English only) descriptions of patient symptoms. This results in 1200 unique datapoints.

The system uses AWS EMR to process the dataset in parallel, creating a set of the diseases and their 10 most likely symptoms. (Note: irrelevant stop words are removed
at the Map stage for better results). 

The results of this procces are stored in a separate EC2 instance. This EC2 is responsible for hosting a Dashboard application written in Flask that parses
the MapReduce output and uses Bootstrap to visualise it as a list for clients.


### Dataset:

<img width="1194" alt="image" src="https://github.com/jamesclackett/Disease-Analysis-Dashboard/assets/55019466/2461de26-969d-4830-9b32-3d39c3698cc4">

### Dashboard:

<img width="1325" alt="image" src="https://github.com/jamesclackett/Disease-Analysis-Dashboard/assets/55019466/9c8f2b82-6f25-48c4-803e-bcd85b3164b9">
