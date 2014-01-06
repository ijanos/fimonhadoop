package fim.apriori.common.trie;

public class Trie<T> {
	private final Node<T> root;

	public Trie() {
		root = new Node<T>(null);
	}

	public void insert(final T[] values) {
		Node<T> current = root;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				final Node<T> child = current.findChild(values[i]);
				if (child != null) {
					current = child;
				} else {
					current = current.addChild(values[i]);
				}
				if (i == values.length - 1) {
					if (!current.isEnd()) {
						current.setEnd();
					}
				}
			}
		}
	}

	public Node<T> getRoot() {
		return this.root;
	}
}
