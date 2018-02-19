#ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H
#define CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H


#include <climits>
#include <vector>
#include <iostream>



namespace cds {
    namespace container {


        class BrownHelgaKtree {
        public:
            int k;

            enum {type_node, type_leaf, type_internal};

            struct UpdateStep{};

            struct Clean: public UpdateStep{};

            struct Node {
                int * keys;
                int type;

                Node(int number_of_keys){
                    keys = new int[number_of_keys];
                    type = type_node;
                };

                ~Node(){
                    if (keys) {
                        delete [] keys;
                    }
                }
            };

            struct Leaf : public Node {
                int keyCount;

                Leaf(int keyCount, int number_of_keys):Node(number_of_keys) {
                    this->keyCount = keyCount;
                    type = type_leaf;
                }
            };

            struct InternalNode : public Node {
                Node ** c_nodes;
                UpdateStep pending;

                InternalNode(int number_of_keys):Node(number_of_keys){
                    pending = Clean();
                    c_nodes = new Node*[number_of_keys+1];
                    type = type_internal;
                };

                ~InternalNode(){
                    if (c_nodes){
                        delete [] c_nodes;
                    }
                }
            };

            struct ReplaceFlag: public UpdateStep{
                const Node l;
                const Node p;
                const Node newChild;
                const int pindex;
            };

            struct PruneFlag: public UpdateStep{
                const Node l;
                const Node p;
                const Node gp;
                const UpdateStep ppending;
                const int gpindex;
            };


            struct Mark: public UpdateStep{
                const PruneFlag pending;
            };


            InternalNode *root;
        protected:



        public:
            BrownHelgaKtree():BrownHelgaKtree(3) {


            }

            BrownHelgaKtree(int k) {
                this->k = k;

                root = new InternalNode(k);
                InternalNode *leftmostChild = new InternalNode(k);

                for (int i = 0; i < k; ++i) {
                    root->keys[i] = INT_MAX;
                    leftmostChild->keys[i] = INT_MAX;
                }


                for (int i = 0; i < k+1; ++i) {
                    leftmostChild->c_nodes[i] = new Leaf(0,k);
                }

                root->c_nodes[0] = leftmostChild;

                for (int i = 1; i < k+1; ++i) {
                    root->c_nodes[i] = new Leaf(0, k);
                }
            }

            ~BrownHelgaKtree() {
                if (root) {
                    delete root;
                }
            }

            struct SearchResult{
                Node * gparent;
                Node * parent;
                Node * leaf;
                UpdateStep * ppending;
                UpdateStep * gppending;
                int pindex;
                int gpindex;

                SearchResult(Node * _gparent, Node * _parent, Node * _leaf, UpdateStep * _ppending,
                             UpdateStep * _gppending, int _pindex, int _gpindex){
                    this->parent = _parent;
                    this->gparent = _gparent;
                    this->leaf = _leaf;
                    this->ppending = _ppending;
                    this->gppending = _gppending;
                    this->pindex = _pindex;
                    this->gpindex = _gpindex;
                };

                ~SearchResult() {
                    if (gparent) { delete gparent;}
                    if (parent) { delete parent;}
                    if (leaf) { delete leaf;}
                    if (ppending) { delete ppending;}
                    if (gppending) {delete gppending;}
                }
            };

            SearchResult * search(int key){
                Node * gparent = root;
                Node * parent = root;
                Node * leaf = (InternalNode*)parent->c_nodes[0];
                UpdateStep gppending = parent->pending;
                UpdateStep ppending = parent->pending;
                int gpindex = 1;
                int pindex = 1;
                while (leaf->type == type_internal){
                    gparent = parent;
                    gppending = ppending;
                    parent = leaf;
                    ppending = parent->pending;
                    gpindex = pindex;
                    //TODO добавить правильный поиск ребенка
                }

            }

        };

    }
}

int main() {
    auto *brownHelgaKtree = new cds::container::BrownHelgaKtree();
    return 0;
//    std::cout <<NULL;
}

#endif // #ifndef CDSLIB_CONTAINER_BROWN_HELGA_KTREE_H