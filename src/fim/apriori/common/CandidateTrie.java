package fim.apriori.common;

import java.util.HashMap;
import java.util.Set;

public class CandidateTrie<T> {

	private final HashMap<T, CandidateTrie<T>> children;
	private int count = -1;

	public CandidateTrie() {
		children = new HashMap<T, CandidateTrie<T>>();
	}

	public boolean contains(final T item) {
		return children.containsKey(item);
	}

	public CandidateTrie<T> get(final T item) {
		if (children.containsKey(item)) {
			return children.get(item);
		} else {
			return null;
		}
	}

	public CandidateTrie<T> find(final T[] items) {
		CandidateTrie<T> node = this;
		for (final T item : items) {
			if (node.children.containsKey(item)) {
				node = node.children.get(item);
			} else {
				return null;
			}
		}
		return node;
	}

	public void increment() {
		count = count + 1;
	}

	public int getCount() {
		return count;
	}

	public Set<T> getChildren() {
		return children.keySet();
	}

	public boolean isLeaf() {
		return children.isEmpty();
	}

	public void insert(final T[] items, final int value) {
		CandidateTrie<T> node = this;
		int i = 0;
		final int n = items.length;

		if (n == 0) {
			return;
		}

		while (i < n) {
			if (node.children.containsKey(items[i])) {
				node = node.children.get(items[i]);
				i = i + 1;
			} else {
				break;
			}
		}

		while (i < n) {
			node.children.put(items[i], new CandidateTrie<T>());
			node = node.children.get(items[i]);
			i = i + 1;
		}

		node.count = value;
	}
}
