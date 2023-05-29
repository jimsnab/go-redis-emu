package redisemu

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestAvlInsertLL(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(20)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(10)
	tree.printTree("-----------------")
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertLR(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(10)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(20)
	tree.printTree("-----------------")
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertRL(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(10)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(20)
	tree.printTree("-----------------")
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertRR(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(10)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(20)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Add(30)
	tree.printTree("-----------------")
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(2461)
	tree.printTree("-----------------")
	tree.Add(1902)
	tree.printTree("-----------------")
	tree.Add(2657)
	tree.printTree("-----------------")
	tree.Add(7812)
	tree.printTree("-----------------")
	tree.Add(4865)
	tree.printTree("-----------------")
	tree.Add(7999)

	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel2(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(686)
	tree.printTree("-----------------")
	tree.Add(959)
	tree.printTree("-----------------")
	tree.Add(1522)
	tree.printTree("-----------------")
	tree.Add(7275)
	tree.printTree("-----------------")
	tree.Add(7537)
	tree.printTree("-----------------")
	tree.Add(5749)

	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel3(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(7150)
	tree.printTree("------------")
	tree.Add(6606)
	tree.printTree("------------")
	tree.Add(2879)
	tree.printTree("------------")
	tree.Add(6229)
	tree.printTree("------------")
	tree.Add(5222)
	tree.printTree("------------")
	tree.Add(7150)

	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel4(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(5499)
	tree.printTree("------------")
	tree.Add(7982)
	tree.printTree("------------")
	tree.Add(7434)
	tree.printTree("------------")
	tree.Add(2050)
	tree.printTree("------------")
	tree.Add(2142)
	tree.printTree("------------")
	tree.Add(6523)

	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel5(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(2249)
	tree.printTree("------------")
	tree.Add(5158)
	tree.printTree("------------")
	tree.Add(6160)
	tree.printTree("------------")
	tree.Add(4987)
	tree.printTree("------------")
	tree.Add(896)
	tree.printTree("------------")
	tree.Add(658)
	tree.printTree("------------")
	tree.Add(7425)
	tree.printTree("------------")
	tree.Add(7866)

	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlDeleteRoot(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.root != nil {
		t.Fatal("not deleted")
	}
}

func TestAvlDeleteLeft(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	tree.Add(20)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(20)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}
}

func TestAvlDeleteRight(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	tree.Add(40)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(40)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}
}

func TestAvlDeletePromoteLeft(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	tree.Add(20)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}

	if tree.root.key != 20 {
		t.Fatal("unexpected root key")
	}
}

func TestAvlDeletePromoteRight(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	tree.Add(40)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}

	if tree.root.key != 40 {
		t.Fatal("unexpected root key")
	}
}

func TestAvlDeletePromoteLeftFull(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.Add(30)
	tree.Add(20)
	tree.Add(40)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(30)
	if !tree.isValid() {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.countEach() != 2 {
		t.Fatal("not deleted")
	}

	if tree.root.key != 20 {
		t.Fatal("unexpected root key")
	}
}

func TestAvlInsertDelete5(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(2460)
	tree.printTree("------------")
	tree.Add(7435)
	tree.printTree("------------")
	tree.Add(2460)

	if !tree.isValid() {
		t.Fatal("imbalanced")
	}

	tree.printTree("------------")
	tree.Delete(-2460)

	if !tree.isValid() {
		t.Fatal("imbalanced")
	}

	tree.printTree("------------")
	tree.Delete(2460)

	if !tree.isValid() {
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertDelete6(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(7472)
	tree.printTree("------------")
	tree.Add(2576)
	tree.printTree("------------")
	tree.Add(2813)
	tree.printTree("------------")
	tree.Add(5622)
	tree.printTree("------------")
	tree.Add(7109)
	tree.printTree("------------")
	tree.Delete(2576)
	tree.printTree("------------")

	if !tree.isValid() {
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertDelete22(t *testing.T) {
	tree := NewAvlTree[float64]()

	tree.printTree("------------")
	tree.Add(743)
	tree.printTree("------------")
	tree.Add(6999)
	tree.printTree("------------")
	tree.Add(7700)
	tree.printTree("------------")
	tree.Add(5829)
	tree.printTree("------------")
	tree.Add(5898)
	tree.printTree("------------")
	tree.Add(7508)
	tree.printTree("------------")
	tree.Delete(5898)
	tree.printTree("------------")
	tree.Delete(6999)
	tree.printTree("------------")
	tree.Add(5096)
	tree.printTree("------------")
	tree.Add(5766)
	tree.printTree("------------")
	tree.Add(7801)
	tree.printTree("------------")
	tree.Add(5557)
	tree.printTree("------------")
	tree.Add(6492)
	tree.printTree("------------")
	tree.Delete(5766)
	tree.printTree("------------")
	tree.Delete(743)
	tree.printTree("------------")
	tree.Add(4230)
	tree.printTree("------------")
	tree.Add(2066)
	tree.printTree("------------")
	tree.Add(1668)
	tree.printTree("------------")
	tree.Delete(5829)
	tree.printTree("------------")
	tree.Add(3929)
	tree.printTree("------------")
	tree.Add(2455)
	tree.printTree("------------")
	tree.Add(2580)
	tree.printTree("------------")

	if !tree.isValid() {
		t.Fatal("imbalanced")
	}

}

func testInsertDelete(worst int) (out []int) {
	history := make([]int, 0, 1024)
	historyPtr := &history

	defer func() {
		if r := recover(); r != nil {
			out = *historyPtr
		}
	}()

	tree := NewAvlTree[float64]()
	numbers := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		op := rand.Intn(4)
		var v int
		if op == 0 && len(numbers) > 0 {
			n := rand.Intn(len(numbers))
			v = -numbers[n]
			if n+1 < len(numbers) {
				numbers = append(numbers[0:n], numbers[n+1:]...)
			} else {
				numbers = numbers[0:n]
			}
		} else {
			v = rand.Intn(8192) + 1
		}

		*historyPtr = append(*historyPtr, v)
		if v > 0 {
			numbers = append(numbers, v)
			tree.Add(float64(v))
		} else {
			tree.Delete(float64(-v))
		}
		if !tree.isValid() {
			if worst == 0 || len(*historyPtr) < worst {
				out = *historyPtr
			}
			break
		}
	}

	return
}

func TestAvlInsertDeleteRandom(t *testing.T) {
	var worst []int
	for pass := 0; pass < 2500; pass++ {
		worst = testInsertDelete(len(worst))
		if len(worst) > 0 {
			break
		}
	}

	if worst != nil {
		tree := NewAvlTree[float64]()
		for _, v := range worst {
			fmt.Println("tree.printTree(\"------------\")")
			if v > 0 {
				fmt.Printf("tree.Add(%v)\n", v)
			} else {
				fmt.Printf("tree.Delete(%v)\n", -v)
			}
			tree.Add(float64(v))
		}
		tree.printTree("------imbalanced------")
		fmt.Printf("%d steps\n", len(worst))
		t.Fatal("imbalanced")
	}
}
