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

public class LockBasedFriendlyTreeSet extends AbstractCompositionalIntSet {

	static final boolean useFairLocks = false;
	static final boolean allocateOutside = true;
	// we encode directions as characters
	static final char Left = 'L';
	static final char Right = 'R';

	private class MaintenanceThread extends Thread {
		LockBasedFriendlyTreeSet map;

		MaintenanceThread(LockBasedFriendlyTreeSet map) {
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

		class BalanceVars {
			volatile int localh, lefth, righth;
		}

		final BalanceVars bal = new BalanceVars();
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
			this.bal.localh = 1;
			this.bal.righth = 0;
			this.bal.lefth = 0;
		}

		Node(final int key, final int localh, final int lefth, final int righth,
				final boolean deleted, final Node left, final Node right) {
			this.key = key;
			this.bal.localh = localh;
			this.bal.righth = righth;
			this.bal.lefth = lefth;
			this.deleted = deleted;
			this.left = left;
			this.right = right;
			this.lock = new ReentrantLock(useFairLocks);
			this.removed = false;
		}

		void setupNode(final int key, final int localh, final int lefth,
				final int righth, final boolean deleted, final Node left,
				final Node right) {
			this.key = key;
			this.bal.localh = localh;
			this.bal.righth = righth;
			this.bal.lefth = lefth;
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

		void updateLocalh() {
			this.bal.localh = Math.max(this.bal.lefth + 1, this.bal.righth + 1);
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
	public LockBasedFriendlyTreeSet() {
		// temporary
		this.startMaintenance();
	}

	void finishCount(int nodesTraversed) {
		Vars vars = counts.get();
		vars.getCount++;
		vars.nodesTraversed += nodesTraversed;
	}

	@Override
	public boolean containsInt(final int key) {
		Node next, current;
		next = root;
		int rightCmp;

		int nodesTraversed = 0;

		while (true) {
			current = next;
			rightCmp = Integer.compare(key, current.key);
			if (rightCmp == 0) {
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
			if (rightCmp <= 0) {
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
		int rightCmp;

		while (true) {
			current = next;
			rightCmp = Integer.compare(key, current.key);
			if (rightCmp == 0) {
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
			if (rightCmp <= 0) {
				next = current.left;
			} else {
				next = current.right;
			}
			if (next == null) {
				if (rightCmp != 0) {
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
		int rightCmp;
		Node next, current;
		next = root;
		Node n = null;
		// int traversed = 0;

		while (true) {
			current = next;
			// traversed++;
			rightCmp = Integer.compare(key, current.key);
			if (rightCmp == 0) {
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
			if (rightCmp <= 0) {
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
					if (rightCmp <= 0) {
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
					if (rightCmp <= 0) {
						next = current.left;
					} else {
						next = current.right;
					}
					if (next == null) {
						if (rightCmp > 0) {
							next = current.left;
						} else {
							next = current.right;
						}
					}
				}
			}
		}
		if (rightCmp == 0) {
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
			if (rightCmp <= 0) {
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
		if (direction == Left) {
			parent.bal.lefth = n.bal.localh - 1;
		} else {
			parent.bal.righth = n.bal.localh - 1;
		}
		parent.updateLocalh();
		return true;
	}

	int rightRotate(Node parent, char direction, boolean doRotate) {
		Node n, l, lr, r, newNode;
		if (parent.removed)
			return 0;
		n = direction == Left ? parent.left : parent.right;
		if (n == null)
			return 0;
		l = n.left;
		if (l == null)
			return 0;
		if (l.bal.lefth - l.bal.righth < 0 && !doRotate) {
			// should do a double rotate
			return 2;
		}
		if (allocateOutside) {
			newNode = new Node(Integer.MAX_VALUE);
		}
		parent.lock.lock();
		n.lock.lock();
		l.lock.lock();
		lr = l.right;
		r = n.right;
		if (allocateOutside) {
			newNode.setupNode(n.key,
					Math.max(1 + l.bal.righth, 1 + n.bal.righth), l.bal.righth,
					n.bal.righth, n.deleted, lr, r);
		} else {
			newNode = new Node(n.key, Math.max(1 + l.bal.righth,
					1 + n.bal.righth), l.bal.righth, n.bal.righth, n.deleted, lr,
					r);
		}
		l.right = newNode;
		n.removed = true;
		if (direction == Left) {
			parent.left = l;
		} else {
			parent.right = l;
		}
		l.lock.unlock();
		n.lock.unlock();
		parent.lock.unlock();
		// need to update balance values
		l.bal.righth = newNode.bal.localh;
		l.updateLocalh();
		if (direction == Left) {
			parent.bal.lefth = l.bal.localh;
		} else {
			parent.bal.righth = l.bal.localh;
		}
		parent.updateLocalh();
		if (STRUCT_MODS) {
			vars.rotations++;
			counts.get().structMods++;
		}
		// System.out.println("right rotate");
		return 1;
	}

	int leftRotate(Node parent, char direction, boolean doRotate) {
		Node n, r, rl, l, newNode;
		if (parent.removed)
			return 0;
		n = direction == Left ? parent.left : parent.right;
		if (n == null)
			return 0;
		r = n.right;
		if (r == null)
			return 0;
		if (r.bal.lefth - r.bal.righth > 0 && !doRotate) {
			// should do a double rotate
			return 3;
		}
		if (allocateOutside) {
			newNode = new Node(Integer.MAX_VALUE);
		}
		parent.lock.lock();
		n.lock.lock();
		r.lock.lock();
		rl = r.left;
		l = n.left;
		if (allocateOutside) {
			newNode.setupNode(n.key,
					Math.max(1 + r.bal.lefth, 1 + n.bal.lefth), n.bal.lefth,
					r.bal.lefth, n.deleted, l, rl);
		} else {
			newNode = new Node(n.key, Math.max(1 + r.bal.lefth,
					1 + n.bal.lefth), n.bal.lefth, r.bal.lefth, n.deleted, l, rl);
		}
		r.left = newNode;

		// temp (Need to fix this!!!!!!!!!!!!!!!!!!!!)
		n.right = parent;
		n.left = parent;

		n.removed = true;
		if (direction == Left) {
			parent.left = r;
		} else {
			parent.right = r;
		}
		r.lock.unlock();
		n.lock.unlock();
		parent.lock.unlock();
		// need to update balance values
		r.bal.righth = newNode.bal.localh;
		r.updateLocalh();
		if (direction == Left) {
			parent.bal.lefth = r.bal.localh;
		} else {
			parent.bal.righth = r.bal.localh;
		}
		parent.updateLocalh();
		if (STRUCT_MODS) {
			vars.rotations++;
			counts.get().structMods++;
		}
		// System.out.println("left rotate");
		return 1;
	}

	boolean propagate(Node node) {
		Node lchild, rchild;

		lchild = node.left;
		rchild = node.right;

		if (lchild == null) {
			node.bal.lefth = 0;
		} else {
			node.bal.lefth = lchild.bal.localh;
		}
		if (rchild == null) {
			node.bal.righth = 0;
		} else {
			node.bal.righth = rchild.bal.localh;
		}

		node.updateLocalh();
		if (STRUCT_MODS)
			vars.propogations++;

		if (Math.abs(node.bal.righth - node.bal.lefth) >= 2)
			return true;
		return false;
	}

	boolean performRotation(Node parent, char direction) {
		int ret;
		Node node;

		ret = singleRotation(parent, direction, false, false);
		if (ret == 2) {
			// Do a LRR
			node = direction == Left ? parent.left : parent.right;
			ret = singleRotation(node, Left, true, false);
			if (ret > 0) {
				if (singleRotation(parent, direction, false, true) > 0) {
					// System.out.println("LRR");
				}
			}
		} else if (ret == 3) {
			// Do a RLR
			node = direction == Left ? parent.left : parent.right;
			ret = singleRotation(node, Right, false, true);
			if (ret > 0) {
				if (singleRotation(parent, direction, true, false) > 0) {
					// System.out.println("RLR");
				}
			}
		}
		if (ret > 0)
			return true;
		return false;
	}

	int singleRotation(Node parent, char direction, boolean leftRotation,
			boolean rightRotation) {
		int bal, ret = 0;
		Node node, child;

		node = direction == Left ? parent.left : parent.right;
		bal = node.bal.lefth - node.bal.righth;
		if (bal >= 2 || rightRotation) {
			// check reiable and rotate
			child = node.left;
			if (child != null) {
				if (node.bal.lefth == child.bal.localh) {
					ret = rightRotate(parent, direction, rightRotation);
				}
			}
		} else if (bal <= -2 || leftRotation) {
			// check reliable and rotate
			child = node.right;
			if (child != null) {
				if (node.bal.righth == child.bal.localh) {
					ret = leftRotate(parent, direction, leftRotation);
				}
			}
		}
		return ret;
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

		if (stop) {
			return true;
		}

		// no rotations for now
		if (!node.removed && node != this.root) {
			if (propagate(node)) {
				this.performRotation(parent, direction);
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
		System.out.println("Total depth: " + recursiveDepth(root.left));
        System.out.println("Average depth: " + averageDepth());
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
        return totalDepth(root, 0) / size();
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
