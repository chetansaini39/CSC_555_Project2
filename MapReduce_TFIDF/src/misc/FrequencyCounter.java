package misc;

import java.util.HashMap;

/**
 * Created by csaini on 5/27/2016.
 */
public class FrequencyCounter {

    /**
     * Method to calculate TF
     */
    public HashMap<String,Double> calculateTermFrequencies(String[] terms) {
        HashMap<String,Double> TF_hashMap=new HashMap<String,Double>();
        for (String term : terms) {
            double frequency = findTermFreqInArray(terms, term);
            TF_hashMap.put(term, frequency);
        }
        return TF_hashMap;
    }


    /**
     * Method to calculate term frequency of a term in an array
     *
     * @param arr
     * @param term
     * @return
     */
    private double findTermFreqInArray(String[] arr, String term) {
        double frequency = 0;
        for (String string : arr) {
            if (string.equalsIgnoreCase(term)) {
                frequency++;
            }
        }
        return frequency / arr.length;
    }
}
