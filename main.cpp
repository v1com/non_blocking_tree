#ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H
#define CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H


#include <climits>
#include <atomic>
#include <vector>
#include <iostream>
#include <algorithm>


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
                    return keyCount.load() > 0;
                }

                bool contains(int key){
                    for (int i = 0; i < keyCount; ++i) {
                        if (keys.load()[i] == key) return true;
                    }
                    return false;
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

                int number_of_non_empty_node() {
                    int i = 0;
                    for (i = 0; i < this->number_of_keys + 1; ++i) {
                        if (c_nodes[i].load()->type == type_internal) {
                            int is_empty_node = dynamic_cast<InternalNode *>(c_nodes[i].load())->number_of_non_empty_node();
                            if (is_empty_node > 0) {
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
                int gpindex = 1;
                int pindex = 1;
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
                if (result || (newValue.load()->type == type_mark and dynamic_cast<PruneFlag*>(newValue.load())->ppending.load() == op.load()))
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
                int non_empty_node = dynamic_cast<InternalNode *>(op.load()->p.load())->number_of_non_empty_node();
                Node *other;
                if (non_empty_node == -1) {
                    other = dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[0].load();
                } else {
                    other = dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[non_empty_node].load();
                }
                Node *node_0 = op.load()->p.load();
                Node *node_1 = other;
                dynamic_cast<InternalNode *>(op.load()->gp.load())->c_nodes[op.load()->gpindex.load()].
                        compare_exchange_strong(node_0, node_1);


                UpdateStep *node_2 = op.load();
                UpdateStep *node_3 = new Clean();
                dynamic_cast<InternalNode *>(op.load()->gp.load())->pending.
                        compare_exchange_strong(node_2, node_3);
            }

            void help_replace(std::atomic<ReplaceFlag *> &op) {
                Node *l = op.load()->l.load();
                Node *newChild = op.load()->newChild.load();

                dynamic_cast<InternalNode *>(op.load()->p.load())->c_nodes[op.load()->pindex.load()].
                        compare_exchange_strong(l, newChild);

                UpdateStep *a = op.load();

                dynamic_cast<InternalNode *>(op.load()->p.load())->pending.
                        compare_exchange_strong(a, new Clean());

            }

            bool find(int key){
                SearchResult * searchResult = search(key);
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

                    if (l.load()->contains(key)){
                        return false;
                    }
                    if (ppending.load()->type != type_clean){
                        help(ppending);
                    } else {
                        if (l.load()->keyCount == k){
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
                for (int i = 0; i < node->number_of_keys; ++i) {
                    if (node->c_nodes[i].load()->type == type_internal)
                        count ++;
                    else if (node->c_nodes[i].load()->type == type_leaf) {
                        if (dynamic_cast<Leaf *> (node->c_nodes[i].load())->keyCount > 0)
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

                    if (!l.load()->contains(key))
                        return false;

                    if (gppending.load()->type != type_clean) {
                        help(gppending);
                    } else if (ppending.load()->type != type_clean) {
                        help(ppending);
                    } else {
                        int ccount = non_empty_childs_count(dynamic_cast<InternalNode *> (p.load()));
                        if (ccount == 2 && l.load()->keyCount == 1) { //pruning deletion
                            std::atomic <PruneFlag *> op(new PruneFlag(l.load(), p.load(), gp.load(), ppending.load(), gpindex.load()));
                            auto expectedOp = gppending.load();

                            bool result = dynamic_cast<InternalNode*>(gp.load())->pending.compare_exchange_strong(expectedOp, op.load());

                            if (result) {
                                if (help_prune(op))
                                    return true;
                                else
                                    help(dynamic_cast<InternalNode*> (gp.load())->pending);
                            }
                        } else { //simple deletion
                            auto * newLeaf = new Leaf(l.load()->keyCount.load() - 1, l.load()->number_of_keys);
                            int * newKeys = new int[newLeaf->keyCount.load()];
                            for (int i = 0; i < l.load()->keyCount.load(); i++) {
                                if (l.load()->keys.load()[i] == key) {
                                    for (int j = 0; j < newLeaf->keyCount; j++) {
                                        if (j < i)
                                            newKeys[j] = l.load()->keys.load()[i];
                                        else
                                            newKeys[j] = l.load()->keys.load()[i + 1];
                                    }
                                    break;
                                }
                            }
                            newLeaf->keys.store(newKeys);

                            std::atomic <Node *> newChild(dynamic_cast<Node *> (newLeaf));
                            std::atomic<ReplaceFlag *> op(new ReplaceFlag(l, p, newChild, pindex));

                            UpdateStep *expectedOp = ppending.load();

                            bool result = dynamic_cast<InternalNode*>(gp.load())->pending.
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

int main() {
    auto *brownHelgaKtree = new cds::container::BrownHelgaKtree(4);

    int SIZE = 100;

    for (int i = 0; i < SIZE; ++i) {
//        if (i == SIZE - 1) {
//            int res = brownHelgaKtree->insert(i);
//        }
        std::cout << i << " inserted with "<<brownHelgaKtree->insert(i)  << std::endl;
    }

    std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << std::endl;
    std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << std::endl;

    for (int i = 0; i < SIZE; ++i) {
        std::cout << i << " found with "<<brownHelgaKtree->find(i)  << std::endl;
    }

    for (int i = 0; i < SIZE; ++i) {
//        if (i == SIZE - 1) {
//            int res = brownHelgaKtree->insert(i);
//        }
        std::cout << i << " inserted with "<<brownHelgaKtree->insert(i)  << std::endl;
    }

    std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << std::endl;
    std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << std::endl;

    for (int i = 0; i < SIZE; ++i) {
        std::cout << i << " found with "<<brownHelgaKtree->find(i)  << std::endl;
    }

    for (int i = 0; i < SIZE; ++i) {
        std::cout << i << " deleted with "<< brownHelgaKtree->remove(i) << std::endl;
    }

    for (int i = 0; i < SIZE; ++i) {
        std::cout << i << " found with "<<brownHelgaKtree->find(i)  << std::endl;
    }

//    brownHelgaKtree->insert(1);
//    std::cout << brownHelgaKtree->find(1) << std::endl;
//    std::cout << "true is " << true << std::endl;
    return 0;
//    std::cout <<NULL;
}

#endif // #ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H