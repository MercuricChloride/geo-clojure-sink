SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE TABLE public.accounts (
    id text NOT NULL,
    avatar text
);


CREATE TABLE public.actions (
    id text NOT NULL,
    action_type text NOT NULL,
    entity text NOT NULL,
    attribute text,
    value_type text,
    value_id text,
    number_value text,
    string_value text,
    entity_value text,
    array_value text[],
    proposed_version_id text,
    version_id text
);


CREATE TABLE public.cursors (
    id integer NOT NULL,
    cursor text NOT NULL,
    block_number text
);

COMMENT ON TABLE public.cursors IS '@name substreamCursor';


CREATE TABLE public.entities (
    id text NOT NULL,
    name character varying,
    description character varying,
    is_type boolean DEFAULT false,
    is_attribute boolean DEFAULT false,
    defined_in text,
    attribute_value_type_id text,
    version_id text
);

CREATE TABLE public.log_entries (
    id text NOT NULL,
    created_at_block text NOT NULL,
    uri text NOT NULL,
    created_by text NOT NULL,
    space text NOT NULL,
    mime_type text,
    decoded text,
    json text
);


CREATE TABLE public.proposals (
    id text NOT NULL,
    space text NOT NULL,
    name text,
    description text,
    created_at integer NOT NULL,
    created_at_block integer NOT NULL,
    created_by text,
    status text NOT NULL
);


CREATE TABLE public.proposed_versions (
    id text NOT NULL,
    name text,
    description text,
    created_at integer NOT NULL,
    created_at_block integer NOT NULL,
    created_by text NOT NULL,
    entity text NOT NULL,
    proposal_id text
);

CREATE TABLE public.spaces (
    id text NOT NULL,
    address text NOT NULL,
    created_at_block text,
    is_root_space boolean,
    admins text,
    editor_controllers text,
    editors text,
    entity text,
    cover text
);


CREATE TABLE public.subspaces (
    id text NOT NULL,
    parent_space text NOT NULL,
    child_space text NOT NULL
);

CREATE TABLE public.triples (
    id text NOT NULL,
    entity_id text NOT NULL,
    attribute_id text NOT NULL,
    value_id text NOT NULL,
    value_type text NOT NULL,
    defined_in text NOT NULL,
    is_protected boolean NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    number_value text,
    array_value text,
    string_value text,
    entity_value text
);


CREATE TABLE public.versions (
    id text NOT NULL,
    name text,
    description text,
    created_at integer NOT NULL,
    created_at_block integer NOT NULL,
    created_by text NOT NULL,
    proposed_version text NOT NULL,
    entity_id text
);

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.actions
    ADD CONSTRAINT actions_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.cursors
    ADD CONSTRAINT cursors_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.log_entries
    ADD CONSTRAINT log_entries_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.proposals
    ADD CONSTRAINT proposals_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.proposed_versions
    ADD CONSTRAINT proposed_versions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.spaces
    ADD CONSTRAINT spaces_address_key UNIQUE (address);

ALTER TABLE ONLY public.spaces
    ADD CONSTRAINT spaces_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.subspaces
    ADD CONSTRAINT subspaces_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.triples
    ADD CONSTRAINT triples_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.versions
    ADD CONSTRAINT versions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.proposed_versions
    ADD CONSTRAINT proposed_versions_proposal_fkey FOREIGN KEY (proposal_id) REFERENCES public.proposals(id);

ALTER TABLE ONLY public.actions
    ADD CONSTRAINT actions_proposed_version_in_actions_fkey FOREIGN KEY (proposed_version_id) REFERENCES public.proposed_versions(id);

ALTER TABLE ONLY public.actions
    ADD CONSTRAINT actions_version_in_actions_fkey FOREIGN KEY (version_id) REFERENCES public.versions(id);


ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entity_defined_in_spaces_address_fkey FOREIGN KEY (defined_in) REFERENCES public.spaces(address);

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entity_value_type_entity_id_fkey FOREIGN KEY (attribute_value_type_id) REFERENCES public.entities(id);

ALTER TABLE ONLY public.spaces
    ADD CONSTRAINT spaces_id_entity_id_fkey FOREIGN KEY (id) REFERENCES public.entities(id);

ALTER TABLE ONLY public.subspaces
    ADD CONSTRAINT subspaces_child_space_fkey FOREIGN KEY (child_space) REFERENCES public.spaces(id);

ALTER TABLE ONLY public.subspaces
    ADD CONSTRAINT subspaces_parent_space_fkey FOREIGN KEY (parent_space) REFERENCES public.spaces(id);

ALTER TABLE ONLY public.triples
    ADD CONSTRAINT triples_attribute_entity_id_fkey FOREIGN KEY (attribute_id) REFERENCES public.entities(id);

ALTER TABLE ONLY public.triples
    ADD CONSTRAINT triples_entity_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);

ALTER TABLE ONLY public.triples
    ADD CONSTRAINT triples_entity_value_entity_id_fkey FOREIGN KEY (entity_value) REFERENCES public.entities(id);
ALTER TABLE ONLY public.versions
    ADD CONSTRAINT versions_to_entities_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);



-- Disable All Triggers so we can play fast and loose with foreign keys
ALTER TABLE public.accounts DISABLE TRIGGER ALL;
ALTER TABLE public.actions DISABLE TRIGGER ALL;
ALTER TABLE public.entities DISABLE TRIGGER ALL;
ALTER TABLE public.log_entries DISABLE TRIGGER ALL;
ALTER TABLE public.proposals DISABLE TRIGGER ALL;
ALTER TABLE public.proposed_versions DISABLE TRIGGER ALL;
ALTER TABLE public.triples DISABLE TRIGGER ALL;
ALTER TABLE public.subspaces DISABLE TRIGGER ALL;
ALTER TABLE public.versions DISABLE TRIGGER ALL;

CREATE INDEX idx_entity_attribute ON public.triples(entity_id, attribute_id);
CREATE INDEX idx_entity_attribute_value_id ON public.triples(entity_id, attribute_id, value_id);
