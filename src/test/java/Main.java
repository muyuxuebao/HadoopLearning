
/**
 * Created by muyux on 2015/12/24.
 */
public class Main {
    public static void main(String[] args) {
        String s = "a b  c";
        String ss[] = s.split("( )+");

        System.out.println(ss.length);
        for (String s1 : ss) {
            System.out.print(s1 + " ");
        }
    }
}
