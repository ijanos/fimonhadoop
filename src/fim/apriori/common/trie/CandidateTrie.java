package fim.apriori.common.trie;

public class CandidateTrie {
	private final Trie<String> trie;

	public CandidateTrie() {
		trie = new Trie<String>();
	}

	public void insert(final String[] items) {
		trie.insert(items);
	}

	public Node<String> getRoot() {
		return this.trie.getRoot();
	}
}
