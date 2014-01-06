package fim.apriori.common.trie;

import java.util.ArrayList;
import java.util.List;

public class Node<T> {
	private final T value;
	private int count;
	private boolean isEnd;
	public List<Node<T>> children;

	public Node(final T value) {
		this.value = value;
		this.isEnd = false;
		this.children = new ArrayList<Node<T>>();
	}

	public Node<T> findChild(final T value) {
		if (children != null) {
			for (final Node<T> node : children) {
				if (node.getValue().equals(value)) {
					return node;
				}
			}

		}
		return null;
	}

	public T getValue() {
		return this.value;
	}

	public int getCount() {
		return this.count;
	}

	public List<Node<T>> getChildren() {
		return this.children;
	}

	public boolean isEnd() {
		return this.isEnd;
	}

	public Node<T> addChild(final T value) {
		final Node<T> node = new Node<T>(value);
		children.add(node);
		return node;
	}

	public void setEnd() {
		this.count = 0;
		this.isEnd = true;
	}

	public void increment() {
		if (!this.isEnd) {
			return;
		}
		this.count += 1;
	}
}
