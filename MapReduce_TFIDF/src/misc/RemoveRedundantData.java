package misc;

import org.tartarus.snowball.ext.EnglishStemmer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by csaini on 5/27/2016.
 */
public class RemoveRedundantData {
    EnglishStemmer englishStemmer= new EnglishStemmer();
    public String removeRedundantData(String s) {
        String sTemp = s;
        sTemp = sTemp.replaceAll("[^a-zA-Z0-9 ]", "");
        sTemp = sTemp.replaceAll("\\{.*?\\} ?", "");
        sTemp = sTemp.replaceAll("\\[.*?\\] ?", "");
        sTemp = sTemp.replaceAll("\\===.*?\\=== ?", "");
        sTemp = sTemp.replaceAll("\\b\\w{13,}\\b\\s?", "");//more than 13 characters are removed
        sTemp = sTemp.replaceAll("\\<.*?\\> ?", "");
        return sTemp;
    }

    /**
     * Method to retun a stemmed list. The list contains words which are stemmed and are considered normalized.
     * @param stopWorldRemovedList
     * @return
     */
    public List<String> doStemming(List<String> stopWorldRemovedList)
{

    List<String> stemmedWordList=new ArrayList<>();
    for (String s:stopWorldRemovedList) {
        englishStemmer.setCurrent(s);//set word for stemmin
        englishStemmer.stem(); //stem the word
        stemmedWordList.add(englishStemmer.getCurrent());//add the stemmed word to list
    }
    return stemmedWordList;
}
}
