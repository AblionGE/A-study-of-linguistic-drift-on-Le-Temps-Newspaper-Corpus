This subdirectory contains small scripts and 


For the python scripts, you may have to tweak the ROOT_DIR to correspond to the directory containing the ngram on an hadoop filesystem.

The `hadoop` command must be accessible in the current PATH and you must have executable right to it.

All the following command are compatible with python version 2.7 with the standard libraries.
# note on the plot

# ngram_top

Print the top ngram for all the years

    python2.7 ngram_uniq.py $number_of_ngram_to_print

# ngram_uniq.py

Print the percentage of uniq ngram for all the years. 
This will graph a very simple ascii plot for all the year.

    python2.7 ngram_uniq.py

# ngram_hist.py

Display a plot of the occurence of the ngram over the years

    python2.7 ngram_hist.py 'ngram to plot'
