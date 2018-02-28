#ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H
#define CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H

#include <climits>
#include <atomic>
#include <vector>
#include <iostream>
#include <algorithm>
#include <thread>
#include <random>
//#include "mingw.thread.h"

namespace cds {
    namespace container {
        int * join_and_sort(int * array, int size, int key){
            int * new_array = new int[size+1];
            for (int i = 0; i < size; ++i) {
                new_array[i] = array[i];
            }
            new_array[size] = key;
            std::sort(new_array, new_array+size+1);
            return new_array;
        }


        class BrownHelgaKtree {
        public:
            int k;

            enum {
                type_node, type_leaf, type_internal
            };
            enum {
                type_update_state, type_clean, type_prune, type_replace, type_mark
            };

            struct UpdateStep {
                int type = type_update_state;

                virtual void *support_function() {};

            };

            struct Clean : public UpdateStep {
                Clean() {
                    this->type = type_clean;
                };
            };

            struct Node {
                int type;
                int number_of_keys;
                std::atomic<int *> keys;

                Node(int number_of_keys) {
                    type = type_node;
                    keys.store(new int[number_of_keys]);
                    this->number_of_keys = number_of_keys;
                };

                ~Node() {
                    if (keys) {
//                        delete[] keys;
                    }
                }

                virtual void *support_function() {};
            };

            struct Leaf : public Node {
                std::atomic<int> keyCount;

                Leaf(int keyCount, int number_of_keys) : Node(number_of_keys) {
                    type = type_leaf;
                    this->keyCount.store(keyCount);
                }

                bool is_empty() {
                    return keyCount.load() <= 0;
                }

                bool contains(int key){
                    for (int i = 0; i < keyCount; ++i) {
                        if (keys.load()[i] == key) return true;
                    }
                    return false;
                }

                void print_keys(){
                    std::cout << "keys of " << this << std::endl;
                    for (int i = 0; i < keyCount; ++i) {
                        std::cout << "key[" << i << "] == " << keys.load()[i] << std::endl;
                    }
                    std::cout << "end of printing keys of " << this << std::endl;
                }
            };

            struct InternalNode : public Node {
                std::atomic<Node *> *c_nodes;
                std::atomic<UpdateStep *> pending;

                InternalNode(int number_of_keys) : Node(number_of_keys) {
                    type = type_internal;
                    pending.store(new Clean());
                    c_nodes = new std::atomic<Node *>[number_of_keys + 1];
                };

                ~InternalNode() {
                    if (c_nodes) {
                        delete c_nodes;
                    }
                }

                std::atomic<Node *> &get_c_node(int key, int &pindex) {
                    int i = 0;
                    for (i; i < number_of_keys; ++i) {
                        if (key < this->keys.load()[i]) {
                            pindex = i;
                            return this->c_nodes[i];
                        }
                    }
                    pindex = i;
                    return this->c_nodes[i];
                }

                int number_of_non_empty_node(Node * which_deleting) {
                    int i = 0;
                    for (i = 0; i < this->number_of_keys + 1; ++i) {
                        if (c_nodes[i].load() == which_deleting) continue;
                        if (c_nodes[i].load()->type == type_internal) {
                            int is_empty_node = dynamic_cast<InternalNode *>(c_nodes[i].load())->number_of_non_empty_node(which_deleting);
                            if (is_empty_node >= 0) {
                                return i;
                            } else {
                                continue;
                            }
                        }
                        if (c_nodes[i].load()->type == type_leaf) {
                            if (dynamic_cast<Leaf *>(c_nodes[i].load())->is_empty()) {
                                continue;
                            } else {
                                return i;
                            }
                        }
                    }
                    return -1;
                }
            };

            struct ReplaceFlag : public UpdateStep {
                std::atomic<Node *> l;
                std::atomic<Node *> p;
                std::atomic<Node *> newChild;
                std::atomic<int> pindex;

                ReplaceFlag(Node *l, Node *p, Node *newChild, int pindex) {
                    this->type = type_replace;
                    this->l.store(l);
                    this->p.store(p);
                    this->newChild.store(newChild);
                    this->pindex.store(pindex);
                };

                ~ReplaceFlag() {
//                    if (this->l) { delete this->l; }
//                    if (this->p) { delete this->p; }
//                    if (this->newChild) { delete this->newChild; }
                }
            };

            struct PruneFlag : public UpdateStep {
                std::atomic<Node *> l;
                std::atomic<Node *> p;
                std::atomic<Node *> gp;
                std::atomic<UpdateStep *> ppending;
                std::atomic<int> gpindex;

                PruneFlag(Node *l, Node *p, Node *gp, UpdateStep *ppending, int gpindex) {
                    this->type = type_prune;
                    this->l.store(l);
                    this->p.store(p);
                    this->gp.store(gp);
                    this->ppending.store(ppending);
                    this->gpindex.store(gpindex);
                };

                ~PruneFlag() {
//                    if (this->l) { delete this->l; }
//                    if (this->p) { delete this->p; }
//                    if (this->gp) { delete this->gp; }
                }
            };


            struct Mark : public UpdateStep {
                std::atomic<PruneFlag *> pending;

                Mark(PruneFlag *pending) {
                    this->type = type_mark;
                    this->pending.store(pending);
                };

                ~Mark() {
//                    if (this->pending) { delete this->pending; }
                }
            };


            std::atomic<InternalNode *> root;
        protected:


        public:
            BrownHelgaKtree() : BrownHelgaKtree(3) {


            }

            BrownHelgaKtree(int k) {
                this->k = k;

                root.store(new InternalNode(k));

                std::atomic<InternalNode *> leftmostChild(new InternalNode(k));

                for (int i = 0; i < k; ++i) {
                    root.load()->keys.load()[i] = INT_MAX;
                    leftmostChild.load()->keys.load()[i] = INT_MAX;
                }


                for (int i = 0; i < k + 1; ++i) {
                    leftmostChild.load()->c_nodes[i].store(new Leaf(0, k));
                }

                root.load()->c_nodes[0].store(leftmostChild);

                for (int i = 1; i < k + 1; ++i) {
                    root.load()->c_nodes[i].store(new Leaf(0, k));
                }
            }

            ~BrownHelgaKtree() {
                if (root) {
//                    delete root;
                }
            }

            struct SearchResult {
                std::atomic<Node *> gparent;
                std::atomic<Node *> parent;
                std::atomic<Node *> leaf;
                std::atomic<UpdateStep *> ppending;
                std::atomic<UpdateStep *> gppending;
                std::atomic<int> pindex;
                std::atomic<int> gpindex;

                SearchResult(Node *_gparent, Node *_parent, Node *_leaf, UpdateStep *_ppending,
                             UpdateStep *_gppending, int _pindex, int _gpindex) {
                    this->parent.store(_parent);
                    this->gparent.store(_gparent);
                    this->leaf.store(_leaf);
                    this->ppending.store(_ppending);
                    this->gppending.store(_gppending);
                    this->pindex.store(_pindex);
                    this->gpindex.store(_gpindex);
                };

                ~SearchResult() {
//                    if (gparent) { delete gparent; }
//                    if (parent) { delete parent; }
//                    if (leaf) { delete leaf; }
//                    if (ppending) { delete ppending; }
//                    if (gppending) { delete gppending; }
                }
            };

            SearchResult *search(int key) {
                Node *gparent = root.load();
                Node *parent = root.load();
                Node *leaf = dynamic_cast<InternalNode *>(parent)->c_nodes[0].load();
                UpdateStep *gppending = dynamic_cast<InternalNode *>(parent)->pending.load();
                UpdateStep *ppending = dynamic_cast<InternalNode *>(parent)->pending.load();
                int gpindex = 0;
                int pindex = 0;
                while (leaf->type == type_internal) {
                    gparent = parent;
                    gppending = ppending;
                    parent = leaf;
                    ppending = dynamic_cast<InternalNode *>(parent)->pending.load();
                    gpindex = pindex;
                    leaf = dynamic_cast<InternalNode *>(parent)->get_c_node(key, pindex);
                }
                return new SearchResult(gparent, parent, leaf, ppending, gppending, pindex, gpindex);
            }


            void help(std::atomic<UpdateStep *> &op) {
                if (op.load()->type == type_replace) {
                    std::atomic<ReplaceFlag *> op_0(dynamic_cast<ReplaceFlag *>(op.load()));
                    help_replace(op_0);
                } else if (op.load()->type == type_prune) {
                    std::atomic<PruneFlag *> op_0(dynamic_cast<PruneFlag *>(op.load()));
                    help_prune(op_0);
                } else if (op.load()->type == type_mark) {
                    std::atomic<PruneFlag *> op_0(dynamic_cast<PruneFlag *>(op.load()));
                    help_marked(op_0);
                }
            }

            bool help_prune(std::atomic<PruneFlag *> &op) {
                UpdateStep * us_0 = op.load()->ppending.load();

                bool result = dynamic_cast<InternalNode*>(op.load()->p.load())->pending.
                        compare_exchange_strong(us_0, new Mark(op.load()));

                std::atomic<UpdateStep *> newValue(dynamic_cast<InternalNode*>(op.load()->p.load())->pending.load());
                if (result || (newValue.load()->type == type_mark && dynamic_cast<Mark*>(newValue.load())->pending.load() == op.load()))
                {
                    help_marked(op);
                    return true;
                } else {
                    help(newValue);
                    UpdateStep * us_0 = op.load();
                    dynamic_cast<InternalNode*>(op.load()->gp.load())->pending.
                            compare_exchange_strong(us_0, new Clean);
                    return false;
                }
            }

            void help_marked(std::atomic<PruneFlag *> &op) {
//                std::cout<< "[0] Leaf which we want to delete: " << op.load()->l.load() << std::endl;
                int non_empty_node = dynamic_cast<InternalNode *>(op.load()->p.load())->number_of_non_empty_node(op.load()->l.load());
                Node *other;
                if (non_empty_node == -1) {
                    other = dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[0].load();
                } else {
                    other = dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[non_empty_node].load();
                }

//                std::cout << "[0] Other leaf has adress: " << other << std::endl;



//                dynamic_cast<Leaf*>(other)->print_keys();

                Node *node_0 = op.load()->p.load();
                Node *node_1 = other;

                int count_non_empty = non_empty_childs_count(dynamic_cast<InternalNode*>(node_0));


                Node * cur_node = dynamic_cast<InternalNode *>(op.load()->gp.load())->c_nodes[op.load()->gpindex.load()].load();

                bool res;
                res = dynamic_cast<InternalNode *>(op.load()->gp.load())->c_nodes[op.load()->gpindex.load()].
                        compare_exchange_strong(node_0, node_1);


                UpdateStep *node_2 = op.load();
                UpdateStep *node_3 = new Clean();
                UpdateStep * cur_us = dynamic_cast<InternalNode *>(op.load()->gp.load())->pending.load();

                res = dynamic_cast<InternalNode *>(op.load()->gp.load())->pending.
                        compare_exchange_strong(node_2, node_3);
            }

            void help_replace(std::atomic<ReplaceFlag *> &op) {
                Node *l = op.load()->l.load();
                Node *newChild = op.load()->newChild.load();

                auto t = dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[op.load()->pindex.load()].
                        compare_exchange_strong(l, newChild);

                UpdateStep *a = op.load();

                auto tt = dynamic_cast<InternalNode *>(op.load()->p.load())->pending.
                        compare_exchange_strong(a, new Clean());

            }

            bool find(int key){
                SearchResult * searchResult = search(key);
//                dynamic_cast<Leaf*>(searchResult->leaf.load())->print_keys();
                return dynamic_cast<Leaf*>(searchResult->leaf.load())->contains(key);
            }

            bool insert(int key){
                std::atomic<Node *> p;
                std::atomic<Node *> newChild;
                std::atomic<Leaf *> l;
                std::atomic<UpdateStep*> ppending;
                std::atomic<int> pindex;
                while (true){
                    SearchResult * sr = search(key);
                    p.store(sr->parent.load());
                    l.store(dynamic_cast<Leaf*>(sr->leaf.load()));
                    pindex.store(sr->pindex.load());
                    ppending.store(sr->ppending.load());

                    int * ll = dynamic_cast<Leaf*>(sr->leaf.load())->keys.load();


                    if (l.load()->contains(key)){
                        return false;
                    }
                    if (ppending.load()->type != type_clean){
                        help(ppending);
                    } else {
                        if (l.load()->keyCount.load() == k){
                            InternalNode * nc = new InternalNode(k);
                            int * new_keys = join_and_sort(l.load()->keys.load(),k,key);
                            int * nc_array = new int[k];

                            for (int j = 0; j < k; ++j) {
                                nc_array[j] = new_keys[j+1];
                            }

                            nc->keys.store(nc_array);

                            for (int i = 0; i < k+1; ++i) {
                                Leaf * node = new Leaf(0,k);
                                node->keyCount.store(1);
                                node->keys.load()[0] = new_keys[i];
                                nc->c_nodes[i].store(node);
                            }
                            nc->pending.store(new Clean());
                            newChild.store(nc);
                        } else {
                            int size = l.load()->keyCount.load();
                            Leaf * nc = new Leaf(size+1,k);
                            int * new_keys = join_and_sort(l.load()->keys.load(),size,key);
                            nc->keys.store(new_keys);
                            newChild.store(nc);
                        }
                        std::atomic<ReplaceFlag *> op(new ReplaceFlag(l.load(),p.load(),newChild.load(),pindex.load()));
                        UpdateStep * us = ppending.load();
                        bool result = dynamic_cast<InternalNode*>(p.load())->pending.
                                compare_exchange_strong(us, op.load());
                        if (result){
                            help_replace(op);
                            return true;
                        } else {
                            help(dynamic_cast<InternalNode*>(p.load())->pending);
                        }
                    }

                }
            }

            int non_empty_childs_count(InternalNode * node) {
                int count = 0;
                for (int i = 0; i < node->number_of_keys+1; ++i) {
                    if (node->c_nodes[i].load()->type == type_internal){
                        int c_count = non_empty_childs_count(dynamic_cast<InternalNode*>(node->c_nodes[i].load()));
                        if (c_count > 0) {
                            count ++;
                        }
                    }
                    else if (node->c_nodes[i].load()->type == type_leaf) {
//                        std::cout << "wtf" << i << dynamic_cast<Leaf *> (node->c_nodes[i].load())->keyCount.load() <<std::endl;
//                        std::cout << "value is " << dynamic_cast<Leaf *> (node->c_nodes[i].load())->keys.load()[0] << std:: endl;
                        if (dynamic_cast<Leaf *> (node->c_nodes[i].load())->keyCount.load() > 0)
                            count++;
                    }
                }
                return count;
            }

            bool remove(int key) {
                SearchResult * searchResult;
                std::atomic <Node *> gp, p;
                std::atomic <UpdateStep *> gppending, ppending;
                std::atomic <Leaf *> l;
                std::atomic <int> pindex, gpindex;

                while(true) {
                    searchResult = search(key);
                    gp.store(searchResult->gparent.load());
                    p.store(searchResult->parent.load());
                    gppending.store(searchResult->gppending.load());
                    ppending.store(searchResult->ppending.load());
                    l.store(dynamic_cast<Leaf*>(searchResult->leaf.load()));
                    pindex.store(searchResult->pindex.load());
                    gpindex.store(searchResult->gpindex.load());

                    int * ll = dynamic_cast<Leaf*>(searchResult->leaf.load())->keys.load();

                    if (!l.load()->contains(key))
                        return false;

                    if (gppending.load()->type != type_clean) {
                        help(gppending);
                    } else if (ppending.load()->type != type_clean) {
                        help(ppending);
                    } else {
//                        std::cout << l.load()->keyCount.load() << std::endl;
                        int ccount = non_empty_childs_count(dynamic_cast<InternalNode *> (p.load()));
                        auto r_m = root.load();
                        auto r_f_c = dynamic_cast<InternalNode*>(root.load())->c_nodes[0].load();

                        auto ref_l = l.load();
                        auto ref_p = p.load();
                        auto ref_gp = gp.load();
                        auto ref_gp_f_c = dynamic_cast<InternalNode*>(ref_gp)->c_nodes[0].load();

                        if (ccount == 2 && l.load()->keyCount.load() == 1) { //pruning deletion
                            std::atomic <PruneFlag *> op(new PruneFlag(l.load(), p.load(), gp.load(), ppending.load(), gpindex.load()));
//                            std::cout << "[0] Removing leaf has adress: " << l.load() << std:: endl;
                            auto * currentOp = dynamic_cast<InternalNode*>(gp.load())->pending.load();
                            auto * expectedOp = gppending.load();
                            auto * tmp_p = p.load();
                            auto * tmp_gp = gp.load();

                            bool result = dynamic_cast<InternalNode*>(gp.load())->pending.compare_exchange_strong(expectedOp, op.load());

                            if (result) {
                                if (help_prune(op))
                                    return true;
                                else
                                    help(dynamic_cast<InternalNode*> (gp.load())->pending);
                            }
                        } else { //simple deletion
                            auto * newLeaf = new Leaf(l.load()->keyCount.load() - 1, k);
                            int * newKeys = new int[newLeaf->keyCount.load()];
                            int * oldKeys = l.load()->keys.load();
                            int j = 0;
                            for (int i = 0; i < l.load()->keyCount.load(); i++) {
                                if (l.load()->keys.load()[i] == key){
                                    continue;
                                }
                                newKeys[j] = oldKeys[i];
                                j++;
                            }
                            newLeaf->keys.store(newKeys);

                            std::atomic <Node *> newChild(dynamic_cast<Node *> (newLeaf));
                            std::atomic<ReplaceFlag *> op(new ReplaceFlag(l, p, newChild, pindex));

                            UpdateStep *expectedOp = ppending.load();

                            bool result = dynamic_cast<InternalNode*>(p.load())->pending.
                                    compare_exchange_strong(expectedOp, op.load());
                            if (result) {
                                help_replace(op);
                                return true;
                            } else {
                                help(dynamic_cast<InternalNode*>(p.load())->pending);
                            }
                        }
                    }
                }
            }
        };

    }
}
void thread_insert(int n, int size, cds::container::BrownHelgaKtree *tree) {
    for (int i = 0; i < size; ++i) {
        std::string log = "thread_" + std::to_string(n);
        if (tree->insert(i))
            log += " inserted ";
        else
            log += " did not insert ";

        log += std::to_string(i) + "\n";
        std::cout << log;
    }
}

void thread_random_insert(int n, int size, cds::container::BrownHelgaKtree *tree) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, size - 1);

    for (int i = 0; i < size; ++i) {
        std::string log = "thread_" + std::to_string(n);
        int rand = dist(gen);
        if (tree->insert(rand))
            log += " inserted ";
        else
            log += " did not insert ";

        log += std::to_string(rand) + "\n";
        std::cout << log;
    }
}

void thread_random_remove(int n, int size, cds::container::BrownHelgaKtree *tree) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, size - 1);

    for (int i = 0; i < size; ++i) {
        std::string log = "thread_" + std::to_string(n);
        int rand = dist(gen);
        if (tree->remove(rand))
            log += " removed ";
        else
            log += " did not remove ";

        log += std::to_string(rand) + "\n";
        std::cout << log;
    }
}

void thread_remove(int n, int size, cds::container::BrownHelgaKtree *tree) {
    for (int i = 0; i < size; ++i) {
        std::string log = "thread_" + std::to_string(n);
        if (tree->remove(i))
            log += " removed ";
        else
            log += " did not remove ";

        log += std::to_string(i) + "\n";
        std::cout << log;
    }
}

void found_test() {
    auto *brownHelgaKtree = new cds::container::BrownHelgaKtree(4);
    int SIZE = 100;
    std::thread t1(thread_insert, 1, SIZE, brownHelgaKtree);
    std::thread t2(thread_insert, 2, SIZE, brownHelgaKtree);
    std::thread t3(thread_random_insert, 3, SIZE, brownHelgaKtree);
    t1.join();
    t2.join();
    t3.join();

    bool allFound = true;
    for (int i = 0; i < SIZE; i++) {
        if (!brownHelgaKtree->find(i))
            allFound = false;
    }
    std::cout << std::endl << (allFound ? "ALL KEYS FOUND" : "FIND TEST FAILED") << std::endl;
}

void insert_with_remove_test() {
    auto *brownHelgaKtree = new cds::container::BrownHelgaKtree(4);
    int SIZE = 100;
    std::thread t1(thread_insert, 1, SIZE, brownHelgaKtree);
    std::thread t2(thread_random_remove, 2, SIZE, brownHelgaKtree);
    std::thread t3(thread_random_insert, 3, SIZE, brownHelgaKtree);
  //  std::thread t4(thread_remove, 4, SIZE, brownHelgaKtree);

    t1.join();
    t2.join();
    t3.join();
 //   t4.join();
}

int main() {
    std::cout << "**********************FOUND TEST STARTED**********************" << std::endl;
    found_test();
    std::cout << "**********************INSERT WITH REMOVE TEST STARTED**********************" << std::endl;
    insert_with_remove_test();
    return 0;
}

#endif // #ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H