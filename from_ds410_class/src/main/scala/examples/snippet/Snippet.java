package snippet;

public class Snippet {
	public String modified(final String input){
	    final StringBuilder builder = new StringBuilder();
	    for(final char c : input.toCharArray())
	        if(Character.isLetterOrDigit(c))
	            builder.append(Character.isLowerCase(c) ? c : Character.toLowerCase(c));
	    return builder.toString();
	}
}

