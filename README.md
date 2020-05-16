
# Multiple Sub-iterable Webscrapping with Threading and Elastic Search.
## Near-Production ready code.
       - Akhil Dudhe

-----------------------------
### Objectives of this project to build Data pipline for iterable webscrapping with the help of scalable solutions around. The project focus on following:
1. Offers python solution for Webscrapping with Multi-Threading to achive parallelism. 
2. Offering fast query solution using Elastic Search.
-----------------------------
#### Problem Statement  : Building a repository of universities of a particular country.
#### Problem Assumption : Country selected as <a href="https://en.wikipedia.org/wiki/List_of_universities_in_England">United Kingdom</a>.
-----------------------------


<ol type="number">
    <li>There are no NAN values in the dataset.</li>
    <li>Out of 59 dataframe columns there are 20 number of columns for Binary variable.</li>
    <li>Out of 59 dataframe columns there are 27 number of columns for Categorical variable</li>
    <li> Out of 27 Ordinal column variables there are 8 such column where the ordered value contains negative numbers.</li>
    <li>Out of 20 Binary column variables there are 4 such column where number of zeros contains 99% of the data. These colums should be discarded as they have no significant effect on modelling.</li>
    <li> The Target variable contains 96.355248 percentage of zeros and 3.644752 percentage of ones respectively.</li>
     <li>To balance the target variable we need to downsampling the majority class in the target variable.</li>
    <li>Function 'downsample' converts the dataframe into desired acheiving balance of the minority class.</li>
    <li>The dataframe consist of no two columns are strongly related that can satify an absolute corelation greater than value 0.5</li>
    <li>From Interval variable column type, column 'ps_ind_01', 'ps_car_01_cat','ps_car_04_cat' and 'ps_car_05_cat' should be consider for StandardScaler.</li>
    </ol>