package trees.lockbased;

import contention.abstractions.AbstractCompositionalIntSet;
import contention.abstractions.CompositionalMap;
import contention.abstractions.MaintenanceAlg;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The contention-friendly tree implementation of map 
 * as described in:
 *
 * T. Crain, V. Gramoli and M. Ryanla. 
 * A Contention-Friendly Binary Search Tree. 
 * Euro-Par 2013.
 * 
 * @author Tyler Crain
 * 
 */

public class LockBasedFriendlyTreeSetNoRotation extends AbstractCompositionalIntSet implements
		MaintenanceAlg {

	static final boolean useFairLocks = false;
	static final boolean allocateOutside = true;
	// we encode directions as characters
	static final char Left = 'L';
	static final char Right = 'R';

	private class MaintenanceThread extends Thread {
		LockBasedFriendlyTreeSetNoRotation map;

		MaintenanceThread(LockBasedFriendlyTreeSetNoRotation map) {
			this.map = map;
		}

		public void run() {
			map.doMaintenance();
		}
	}

	private class MaintVariables {
		long propogations = 0, rotations = 0;
	}

	private final MaintVariables vars = new MaintVariables();

	private static class Node {
		int key;

		volatile boolean deleted;

		volatile Node left;
		volatile Node right;
		final ReentrantLock lock;
		volatile boolean removed;

		Node(final int key) {
			this.key = key;
			this.removed = false;
			this.lock = new ReentrantLock(useFairLocks);
			this.right = null;
			this.left = null;
		}

		Node(final int key, final int localh, final int lefth, final int righth,
				boolean deleted, final Node left, final Node right) {
			this.key = key;
			this.deleted = deleted;
			this.left = left;
			this.right = right;
			this.lock = new ReentrantLock(useFairLocks);
			this.removed = false;
		}

		void setupNode(final int key, final int localh, final int lefth,
				boolean deleted, final int righth, final Node left,
				final Node right) {
			this.key = key;
			this.deleted = deleted;
			this.left = left;
			this.right = right;
			this.removed = false;
		}

		Node child(char dir) {
			return dir == Left ? left : right;
		}

		Node childSibling(char dir) {
			return dir == Left ? right : left;
		}

		void setChild(char dir, Node node) {
			if (dir == Left) {
				left = node;
			} else {
				right = node;
			}
		}

	}

	// state
	private final Node root = new Node(Integer.MAX_VALUE);
	volatile boolean stop = false;
	private MaintenanceThread mainThd;
	// used in the getSize function
	int size;
	private long structMods = 0;

	// Constructors
	public LockBasedFriendlyTreeSetNoRotation() {
		// temporary
		this.startMaintenance();
	}

	public boolean containsInt(int key) {
		return get(key);
	}

	void finishCount(int nodesTraversed) {
		Vars vars = counts.get();
		vars.getCount++;
		vars.nodesTraversed += nodesTraversed;
	}

	public boolean get(final int key) {
		Node next, current;
		next = root;

		int nodesTraversed = 0;

		while (true) {
			current = next;
			if (current.key == key) {
				if (current.deleted) {
					if (TRAVERSAL_COUNT) {
						finishCount(nodesTraversed);
					}
					return false;
				}
				if (TRAVERSAL_COUNT) {
					finishCount(nodesTraversed);
				}
				return true;
			}
			if (key <= current.key) {
				next = current.left;
			} else {
				next = current.right;
			}
			if (TRAVERSAL_COUNT) {
				nodesTraversed++;
			}
			if (next == null) {
				if (TRAVERSAL_COUNT) {
					finishCount(nodesTraversed);
				}
				return false;
			}
		}
	}

	@Override
	public boolean removeInt(final int key) {
		Node next, current;
		next = root;

		while (true) {
			current = next;
			if (current.key == key) {
				if (current.deleted) {
					return false;
				}
				current.lock.lock();
				if (!current.removed) {
					break;
				} else {
					current.lock.unlock();
				}
			}
			if (key <= current.key) {
				next = current.left;
			} else {
				next = current.right;
			}
			if (next == null) {
				if (current.key != key) {
					return false;
				}
				// this only happens if node is removed, so you take the
				// opposite path
				// this should never be null
				System.out.println("Going right");
				next = current.right;
			}
		}
		if (current.deleted) {
			current.lock.unlock();
			return false;
		} else {
			current.deleted = true;
			current.lock.unlock();
			// System.out.println("delete");
			return true;
		}
	}

	@Override
	public boolean addInt(int key) {
		Node next, current;
		next = root;
		Node n = null;
		// int traversed = 0;

		while (true) {
			current = next;
			// traversed++;
			if (current.key == key) {
				if (!current.deleted) {
					// System.out.println(traversed);
					return false;
				}
				current.lock.lock();
				if (!current.removed) {
					break;
				} else {
					current.lock.unlock();
				}
			}
			if (key <= current.key) {
				next = current.left;
			} else {
				next = current.right;
			}
			if (next == null) {
				if (n == null && allocateOutside) {
					n = new Node(key);
				}
				current.lock.lock();
				if (!current.removed) {
					if (key <= current.key) {
						next = current.left;
					} else {
						next = current.right;
					}
					if (next == null) {
						break;
					} else {
						current.lock.unlock();
					}
				} else {
					current.lock.unlock();
					// maybe have to check if the other one is still null before
					// going the opposite way?
					// YES!! We do this!
					if (key <= current.key) {
						next = current.left;
					} else {
						next = current.right;
					}
					if (next == null) {
						if (key > current.key) {
							next = current.left;
						} else {
							next = current.right;
						}
					}
				}
			}
		}
		if (current.key == key) {
			if (current.deleted) {
				current.deleted = false;
				current.lock.unlock();
				// System.out.println("insert");
				// System.out.println(traversed);
				return true;
			} else {
				current.lock.unlock();
				return false;
			}
		} else {
			if (!allocateOutside) {
				n = new Node(key);
			}
			if (key <= current.key) {
				current.left = n;
			} else {
				current.right = n;
			}
			current.lock.unlock();
			// System.out.println(traversed);
			// System.out.println("insert");
			return true;
		}
	}

	// maintenance
	boolean removeNode(Node parent, char direction) {
		Node n, child;
		// can get before locks because only maintenance removes nodes
		if (parent.removed)
			return false;
		n = direction == Left ? parent.left : parent.right;
		if (n == null)
			return false;
		// get the locks
		n.lock.lock();
		parent.lock.lock();
		if (!n.deleted) {
			n.lock.unlock();
			parent.lock.unlock();
			return false;
		}
		if ((child = n.left) != null) {
			if (n.right != null) {
				n.lock.unlock();
				parent.lock.unlock();
				return false;
			}
		} else {
			child = n.right;
		}
		if (direction == Left) {
			parent.left = child;
		} else {
			parent.right = child;
		}
		n.left = parent;
		n.right = parent;
		n.removed = true;
		n.lock.unlock();
		parent.lock.unlock();
		// System.out.println("removed a node");
		// need to update balance values here
		return true;
	}

	boolean recursivePropagate(Node parent, Node node,
			char direction) {
		Node left, right;

		if (node == null)
			return true;
		left = node.left;
		right = node.right;

		if (!node.removed && node.deleted
				&& (left == null || right == null) && node != this.root) {
			if (removeNode(parent, direction)) {
				return true;
			}
		}

		if (stop) {
			return true;
		}

		if (!node.removed) {
			if (left != null) {
				recursivePropagate(node, left, Left);
			}
			if (right != null) {
				recursivePropagate(node, right, Right);
			}
		}

		return true;
	}

	public boolean stopMaintenance() {
		this.stop = true;
		try {
			this.mainThd.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	public boolean startMaintenance() {
		this.stop = false;

		mainThd = new MaintenanceThread(this);

		mainThd.start();

		return true;
	}

	boolean doMaintenance() {
		while (!stop) {
			recursivePropagate(this.root, this.root.left, Left);
		}
		if (STRUCT_MODS)
			this.structMods += counts.get().structMods;
		System.out.println("Propogations: " + vars.propogations);
		System.out.println("Rotations: " + vars.rotations);
		System.out.println("Max depth: " + recursiveDepth(root.left));
                System.out.println("Average depth: " + averageDepth());
                System.out.println("Total depth: " + totalDepth(root.left, 0));
                System.out.println("Hash: " + hash());
		return true;
	}

	// not thread safe
	public int getSize() {
		this.size = 0;
		recursiveGetSize(root.left);
		return size;
	}

	void recursiveGetSize(Node node) {
		if (node == null)
			return;
		if (node.removed) {
			// System.out.println("Shouldn't find removed nodes in the get size function");
		}
		if (!node.deleted) {
			this.size++;
		}
		recursiveGetSize(node.left);
		recursiveGetSize(node.right);
	}

	public int numNodes() {
		this.size = 0;
		ConcurrentHashMap<Integer, Node> map = new ConcurrentHashMap<Integer, Node>();
		recursiveNumNodes(root.left, map);
		return size;
	}

	void recursiveNumNodes(Node node,
			ConcurrentHashMap<Integer, Node> map) {
		if (node == null)
			return;
		if (node.removed) {
			// System.out.println("Shouldn't find removed nodes in the get size function");
		}
		Node n = map.putIfAbsent((Integer) node.key, node);
		if (n != null) {
			System.out.println("Error: " + node.key);
		}
		this.size++;
		recursiveNumNodes(node.left, map);
		recursiveNumNodes(node.right, map);
	}

	public int getBalance() {
		int lefth = 0, righth = 0;
		if (root.left == null)
			return 0;
		lefth = recursiveDepth(root.left.left);
		righth = recursiveDepth(root.left.right);
		return lefth - righth;
	}

	int recursiveDepth(Node node) {
		if (node == null) {
			return 0;
		}
		int lefth, righth;
		lefth = recursiveDepth(node.left);
		righth = recursiveDepth(node.right);
		return Math.max(lefth, righth) + 1;
	}

    int totalDepth(Node node, int d) {
        if (node == null) {
            return 0;
        }
        return (!node.deleted ? d : 0) + totalDepth(node.left, d + 1) + totalDepth(node.right, d + 1);
    }

    int averageDepth() {
        return totalDepth(root.left, 0) / size();
    }

        int hash(Node node, int power) {
            if (node == null) {
                return 0;
            }
            return (node.deleted ? 1 : 0) * power + hash(node.left, power * 239) + hash(node.right, power * 533);
        }

        int hash() {
            return hash(root.left, 1);
        }

	@Override
	public void clear() {
		this.stopMaintenance();
		this.resetTree();
		this.startMaintenance();

		return;
	}

	private void resetTree() {
		this.structMods = 0;
		this.vars.propogations = 0;
		this.vars.rotations = 0;
		root.left = null;
	}

	@Override
	public int size() {
		return this.getSize();
	}

	public long getStructMods() {
		return structMods;
	}

}
