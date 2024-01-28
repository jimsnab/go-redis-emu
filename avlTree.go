package redisemu

import (
	"fmt"
	"math"
	"strings"
)

type (
	avlKeyType interface {
		string | float64
	}
	avlNode[T avlKeyType] struct {
		key     T
		left    *avlNode[T]
		right   *avlNode[T]
		parent  *avlNode[T]
		balance int
	}

	avlTree[T avlKeyType] struct {
		root *avlNode[T]
	}

	avlOperation[T avlKeyType] struct {
		key   T
		leaf  *avlNode[T]
		added bool
	}

	AvlIterator[T avlKeyType] func(node *avlNode[T]) bool
)

func NewAvlTree[T avlKeyType]() *avlTree[T] {
	return &avlTree[T]{}
}

// locates a key in the AVL tree
func (tree *avlTree[T]) Find(key T) (node *avlNode[T]) {
	n := tree.root

	for {
		if n == nil {
			return
		}

		if n.key == key {
			node = n
			return
		}

		if n.key > key {
			n = n.left
		} else {
			n = n.right
		}
	}
}

// adds a key to the AVL tree, or finds the existing node
func (tree *avlTree[T]) Add(key T) (node *avlNode[T], added bool) {
	op := &avlOperation[T]{
		key: key,
	}
	tree.root, _ = op.insertNode(nil, tree.root)
	return op.leaf, op.added
}

// removes a key from the AVL tree, returing true if the key was found and deleted
func (tree *avlTree[T]) Delete(key T) bool {
	op := &avlOperation[T]{
		key: key,
	}
	tree.root, _ = op.deleteNode(tree.root)
	return op.leaf != nil
}

// recursive worker that searches for the insertion position for a new node, and adds and rebalances the tree if key doesn't already exist
func (op *avlOperation[T]) insertNode(parent *avlNode[T], node *avlNode[T]) (out *avlNode[T], balanced bool) {
	if node == nil {
		out = &avlNode[T]{
			key:    op.key,
			parent: parent,
		}
		op.leaf = out
		op.added = true
		return
	}

	if op.key == node.key {
		op.leaf = node
		balanced = true
	} else {
		if op.key < node.key {
			node.left, balanced = op.insertNode(node, node.left)
			if !balanced {
				node.balance--
				if node.balance < -1 {
					node = node.rotateLeft(node.left)
				}
				balanced = (node.balance == 0)
			}
		} else {
			node.right, balanced = op.insertNode(node, node.right)
			if !balanced {
				node.balance++
				if node.balance > 1 {
					node = node.rotateRight(node.right)
				}
				balanced = (node.balance == 0)
			}
		}
	}

	out = node
	return
}

// recursive worker that searches for a node, and if found, deletes and rebalances the tree
func (op *avlOperation[T]) deleteNode(node *avlNode[T]) (out *avlNode[T], rebalanced bool) {
	if node == nil {
		rebalanced = true
		return
	}
	if node.key == op.key {
		op.leaf = node
		if node.left == nil {
			out = node.right
			if out != nil {
				out.parent = node.parent
			}
			return
		}
		if node.right == nil {
			out = node.left
			out.parent = node.parent
			return
		}

		replacement := node.left
		for replacement.right != nil {
			replacement = replacement.right
		}

		node.key = replacement.key
		op.key = replacement.key // remove from further down in the tree
	}

	if op.key <= node.key {
		node.left, rebalanced = op.deleteNode(node.left)
		if !rebalanced {
			node.balance++
			if node.balance > 1 {
				node, rebalanced = node.deleteRotateRight(node.right)
			} else {
				rebalanced = (node.balance != 0)
			}
		}
	} else {
		node.right, rebalanced = op.deleteNode(node.right)
		if !rebalanced {
			node.balance--
			if node.balance < -1 {
				node, rebalanced = node.deleteRotateLeft(node.left)
			} else {
				rebalanced = (node.balance != 0)
			}
		}
	}

	out = node
	return
}

// worker to update the balance factor
func (node *avlNode[T]) adjustBalance(second *avlNode[T], third *avlNode[T], direction int) {
	switch third.balance {
	case 0:
		node.balance = 0
		second.balance = 0
	case direction:
		node.balance = 0
		second.balance = -direction
	default:
		node.balance = direction
		second.balance = 0
	}
	third.balance = 0
}

// worker to balance the tree after left insertion makes the tree left heavy
func (node *avlNode[T]) rotateLeft(middle *avlNode[T]) *avlNode[T] {
	nodeParent := node.parent
	if middle.balance < 0 {
		// left-left rotation
		subtreeC := middle.right
		middle.right = node
		node.left = subtreeC
		node.parent = middle
		middle.parent = nodeParent
		node.balance = 0
		middle.balance = 0
		return middle
	} else {
		// left-right rotation
		third := middle.right
		if third == nil {
			//panic("expected non-nil right node")
			return node
		}
		node.adjustBalance(middle, third, 1)
		subtreeB := third.left
		subtreeC := third.right
		third.left = middle
		third.right = node
		middle.right = subtreeB
		node.left = subtreeC
		node.parent = third
		middle.parent = third
		third.parent = nodeParent
		return third
	}
}

// worker to balance the tree after right insertion makes the tree right heavy
func (node *avlNode[T]) rotateRight(middle *avlNode[T]) *avlNode[T] {
	nodeParent := node.parent
	if middle.balance > 0 {
		// right-right rotation
		subtreeB := middle.left
		middle.left = node
		node.right = subtreeB
		node.parent = middle
		middle.parent = nodeParent
		node.balance = 0
		middle.balance = 0
		return middle
	} else {
		// right-left rotation
		third := middle.left
		if third == nil {
			//panic("expected non-nil left node")
			return node
		}
		node.adjustBalance(middle, third, -1)
		subtreeB := third.left
		subtreeC := third.right
		third.left = node
		third.right = middle
		node.right = subtreeB
		middle.left = subtreeC
		node.parent = third
		middle.parent = third
		third.parent = nodeParent
		return third
	}
}

// worker to rotate after a right node deletion leaves the tree unbalanced
func (node *avlNode[T]) deleteRotateLeft(middle *avlNode[T]) (out *avlNode[T], rebalanced bool) {
	if middle.balance == 0 {
		nodeParent := node.parent
		subtreeC := middle.right
		middle.right = node
		node.left = subtreeC
		node.parent = middle
		middle.parent = nodeParent
		node.balance = -1
		middle.balance = 1
		return middle, true
	} else {
		return node.rotateLeft(middle), false
	}
}

// worker to rotate after a left node deletion leaves the tree unbalanced
func (node *avlNode[T]) deleteRotateRight(middle *avlNode[T]) (out *avlNode[T], rebalanced bool) {
	if middle.balance == 0 {
		nodeParent := node.parent
		subtreeB := middle.left
		middle.left = node
		node.right = subtreeB
		node.parent = middle
		middle.parent = nodeParent
		node.balance = 1
		middle.balance = -1
		return middle, true
	} else {
		return node.rotateRight(middle), false
	}
}

// testing function
func (node *avlNode[T]) subtreeHeight() (height int) {
	if node == nil {
		return
	}

	leftHeight := node.left.subtreeHeight()
	rightHeight := node.right.subtreeHeight()

	if leftHeight >= rightHeight {
		return leftHeight + 1
	} else {
		return rightHeight + 1
	}
}

// testing function
func (node *avlNode[T]) isBalanced() bool {
	if node == nil {
		return true
	}

	delta := node.left.subtreeHeight() - node.right.subtreeHeight()
	return delta >= -1 && delta <= 1
}

// testing function
func (node *avlNode[T]) checkBalanceFactors() bool {
	if node == nil {
		return true
	}

	if !node.left.checkBalanceFactors() || !node.right.checkBalanceFactors() {
		return false
	}

	lh := node.left.subtreeHeight()
	rh := node.right.subtreeHeight()

	balance := rh - lh
	return node.balance == balance
}

// testing function
func (node *avlNode[T]) checkParentLinks() bool {
	if node == nil {
		return true
	}

	if node.left != nil && node.left.parent != node {
		return false
	}
	if node.right != nil && node.right.parent != node {
		return false
	}

	return true
}

// testing function
func (tree *avlTree[T]) isValid() bool {
	if !tree.root.checkBalanceFactors() {
		return false
	}
	if !tree.root.checkParentLinks() {
		return false
	}
	return tree.root.isBalanced()
}

// iterates the AVL tree in sorted order
func (tree *avlTree[T]) Iterate(iter AvlIterator[T]) {
	tree.root.iterateNext(iter)
}

func (node *avlNode[T]) iterateNext(iter AvlIterator[T]) bool {
	if node == nil {
		return true
	}

	if node.left != nil {
		if !node.left.iterateNext(iter) {
			return false
		}
	}
	if !iter(node) {
		return false
	}
	if node.right != nil {
		if !node.right.iterateNext(iter) {
			return false
		}
	}
	return true
}

// testing function
func (tree *avlTree[T]) countEach() int {
	count := 0
	tree.Iterate(func(node *avlNode[T]) bool {
		count++
		return true
	})
	return count
}

// testing function
func (tree *avlTree[T]) printTree(header string) {
	fmt.Println(header)
	if tree.root == nil {
		fmt.Println("(nil)")
		return
	}
	maxWidth := 0
	tree.Iterate(func(node *avlNode[T]) bool {
		width := len(fmt.Sprintf("%v", node.balance))
		if width > maxWidth {
			maxWidth = width
		}
		return true
	})

	height := tree.root.subtreeHeight()

	heightExp := math.Pow(2, float64(height)) / 2
	nodeWidth := maxWidth + 2
	fieldWidth := int(heightExp) * nodeWidth

	nextLineNodes := []*avlNode[T]{}
	nextLineNodes = append(nextLineNodes, tree.root)

	for {
		lineNodes := nextLineNodes
		nextLineNodes = []*avlNode[T]{}

		keyLine := ""
		connectorLine := ""
		more := false
		for _, node := range lineNodes {
			kl, cl := node.nodeText(fieldWidth, nodeWidth)
			keyLine += kl
			connectorLine += cl

			if node != nil {
				nextLineNodes = append(nextLineNodes, node.left, node.right)
				more = more || (node.left != nil || node.right != nil)
			} else {
				nextLineNodes = append(nextLineNodes, nil, nil)
			}
		}

		fmt.Println(keyLine)
		if strings.ContainsAny(connectorLine, "/\\") {
			fmt.Println(connectorLine)
		}

		fieldWidth /= 2

		if !more {
			break
		}
	}
}

// testing function
func (node *avlNode[T]) nodeText(fieldWidth, nodeWidth int) (keyLine, connectorLine string) {
	if node == nil {
		keyLine = strings.Repeat(" ", fieldWidth)
		connectorLine = keyLine
	} else {
		leftSpaces := (fieldWidth - nodeWidth) / 2
		connectorLine = strings.Repeat(" ", leftSpaces)
		if node.left != nil {
			connectorLine += "/"
		} else {
			connectorLine += " "
		}
		connectorLine += strings.Repeat(" ", nodeWidth-2)
		if node.right != nil {
			connectorLine += "\\"
		} else {
			connectorLine += " "
		}
		connectorLine += strings.Repeat(" ", fieldWidth-len(connectorLine))

		keyText := fmt.Sprintf("%v", node.balance)
		leftSpaces += (nodeWidth - len(keyText)) / 2
		rightSpaces := fieldWidth - leftSpaces - len(keyText)

		keyLine = strings.Repeat(" ", leftSpaces)
		keyLine += keyText
		keyLine += strings.Repeat(" ", rightSpaces)
	}
	return
}
