package helpers;

public class TextPair {


    String text;
    double num;


    //Default constructor
    public TextPair(){

        text="";
        num=0;


    }

    //Custom contructor
    public TextPair(String blah, int i) {
    }


    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }



}
